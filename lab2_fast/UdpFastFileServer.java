package org.example;

import java.io.*;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;

public final class UdpFastFileServer {

    // ====== TUNING ======
    private static final int PORT_DEFAULT = 50506;
    private static final String DIR_DEFAULT = "server_storage_udp_fast";

    // Под hotspot лучше ставить MTU-safe payload ~1472
    private static final int MAX_PAYLOAD = 1472;

    // Сколько пакетов шлём подряд перед тем как "быстро глянуть" входящие
    private static final int BURST = 2048;

    // Сколько миллисекунд считаем "тишиной", чтобы закончить фазу blast
    private static final int IDLE_FINISH_MS = 120;

    // Ремонт-раундов (NACK -> resend)
    private static final int REPAIR_ROUNDS = 3;

    // Максимум ranges в одном NACK (каждый range = 8 байт: start(int)+len(int))
    private static final int MAX_NACK_RANGES = 2048;

    // Socket buffers (очень влияет на скорость и потери)
    private static final int SO_RCVBUF = 8 * 1024 * 1024;
    private static final int SO_SNDBUF = 8 * 1024 * 1024;

    // ====== PROTOCOL ======
    private static final short MAGIC = (short) 0x5546; // 'U''F'
    private static final byte VER = 1;

    // types
    private static final byte T_REQ_GET  = 1; // client->server: "GET name\nchunk\n"
    private static final byte T_REQ_PUT  = 2; // client->server: "PUT name\nsize\nchunk\n"
    private static final byte T_INFO     = 3; // server->client: meta for transfer
    private static final byte T_DATA     = 4; // data packets
    private static final byte T_NACK     = 5; // receiver->sender: missing ranges
    private static final byte T_DONE     = 6; // sender->receiver: "DONE"
    private static final byte T_ERR      = 7; // error text

    // header:
    // magic(2) ver(1) type(1) transferId(4) seq(4) totalChunks(4) chunk(2) payloadLen(2) = 20 bytes
    private static final int HDR = 20;

    private static final class Packet {
        final byte type;
        final int transferId;
        final int seq;
        final int totalChunks;
        final int chunk;
        final byte[] payload;

        Packet(byte type, int transferId, int seq, int totalChunks, int chunk, byte[] payload) {
            this.type = type;
            this.transferId = transferId;
            this.seq = seq;
            this.totalChunks = totalChunks;
            this.chunk = chunk;
            this.payload = payload;
        }

        String payloadText() {
            return new String(payload, StandardCharsets.UTF_8).trim();
        }
    }

    // ====== STATE (single transfer at a time; single thread) ======
    private static final class Transfer {
        final boolean upload;              // true if client->server (PUT)
        final SocketAddress peer;
        final int transferId;
        final File file;
        final RandomAccessFile raf;
        final long totalBytes;
        final int chunk;
        final int totalChunks;

        final BitSet received;             // which chunks receiver has (for upload: server receives; for download: client receives; but server needs it when it is receiver in upload)
        final BitSet sentOrRepaired;       // for download sender bookkeeping (optional)

        int nextToSend = 0;                // download: next seq to blast
        long lastRxMs = System.currentTimeMillis();

        Transfer(boolean upload, SocketAddress peer, int transferId, File file, RandomAccessFile raf,
                 long totalBytes, int chunk, int totalChunks) {
            this.upload = upload;
            this.peer = peer;
            this.transferId = transferId;
            this.file = file;
            this.raf = raf;
            this.totalBytes = totalBytes;
            this.chunk = chunk;
            this.totalChunks = totalChunks;
            this.received = new BitSet(totalChunks);
            this.sentOrRepaired = new BitSet(totalChunks);
        }

        void touchRx() { lastRxMs = System.currentTimeMillis(); }

        void closeQuiet() { try { raf.close(); } catch (Exception ignored) {} }
    }

    private final File baseDir;
    private final DatagramSocket sock;
    private final Random rnd = new Random();

    private Transfer current = null;

    public UdpFastFileServer(int port, File baseDir) throws Exception {
        this.baseDir = baseDir;
        ensureDir(baseDir);

        this.sock = new DatagramSocket(port);
        this.sock.setReceiveBufferSize(SO_RCVBUF);
        this.sock.setSendBufferSize(SO_SNDBUF);

        // Очень важно: маленький timeout, чтобы можно было interleave send/recv в одном потоке
        this.sock.setSoTimeout(1);
    }

    public void run() throws Exception {
        System.out.println("UDP FAST server on port " + sock.getLocalPort());
        System.out.println("Storage: " + baseDir.getAbsolutePath());
        System.out.println("MAX_PAYLOAD=" + MAX_PAYLOAD + " BURST=" + BURST);

        byte[] buf = new byte[65507];
        DatagramPacket dp = new DatagramPacket(buf, buf.length);

        while (true) {
            // 1) если есть активный download — blast порциями
            if (current != null && !current.upload) {
                blastSomeDownload(current);
                // если давно ничего не приходило (нет NACK) — возможно клиент уже доволен
                // но всё равно ждём NACK/DONE чуть-чуть
            }

            // 2) читаем входящие пакеты (короткий poll)
            Packet p = recvPacket(dp);
            if (p == null) continue;

            SocketAddress peer = dp.getSocketAddress();

            switch (p.type) {
                case T_REQ_GET -> handleGet(peer, p);
                case T_REQ_PUT -> handlePut(peer, p);
                case T_DATA    -> handleData(peer, p);
                case T_NACK    -> handleNack(peer, p);
                case T_DONE    -> handleDone(peer, p);
                default -> { /* ignore */ }
            }
        }
    }

    // ===== GET (download) =====
    private void handleGet(SocketAddress peer, Packet req) throws Exception {
        // формат payload: "name\nchunk\n"
        String[] lines = req.payloadText().split("\n");
        if (lines.length < 2) {
            sendErr(peer, 0, "Bad GET");
            return;
        }
        String name = sanitizeFileName(lines[0].trim());
        int chunk = parseInt(lines[1].trim(), MAX_PAYLOAD);
        if (name == null || chunk <= 0 || chunk > MAX_PAYLOAD) {
            sendErr(peer, 0, "Bad args");
            return;
        }

        File f = new File(baseDir, name);
        if (!f.exists() || !f.isFile()) {
            sendErr(peer, 0, "No such file");
            return;
        }

        long total = f.length();
        int totalChunks = (int) ((total + chunk - 1L) / chunk);
        int tid = rnd.nextInt();

        RandomAccessFile raf = new RandomAccessFile(f, "r");
        Transfer t = new Transfer(false, peer, tid, f, raf, total, chunk, totalChunks);
        current = t;

        // INFO: payload = "OK name size\n"
        String info = "OK " + name + " " + total + "\n";
        sendPacket(peer, T_INFO, tid, 0, totalChunks, chunk, info.getBytes(StandardCharsets.UTF_8));

        // начинаем blast сразу
        t.nextToSend = 0;
        t.touchRx();
    }

    private void blastSomeDownload(Transfer t) throws Exception {
        int sent = 0;
        while (sent < BURST && t.nextToSend < t.totalChunks) {
            int seq = t.nextToSend++;
            // читаем кусок
            long off = (long) seq * t.chunk;
            int len = (int) Math.min(t.chunk, t.totalBytes - off);
            byte[] data = new byte[len];
            t.raf.seek(off);
            t.raf.readFully(data);

            sendPacket(t.peer, T_DATA, t.transferId, seq, t.totalChunks, t.chunk, data);
            sent++;
        }

        // если все отправили — ждём NACK/или DONE. Если тишина — можно доп. DONE.
        long now = System.currentTimeMillis();
        if (t.nextToSend >= t.totalChunks && (now - t.lastRxMs) > IDLE_FINISH_MS) {
            // "подтолкнём" клиента завершиться
            sendPacket(t.peer, T_DONE, t.transferId, 0, t.totalChunks, t.chunk, "DONE".getBytes(StandardCharsets.UTF_8));
            // не закрываем — вдруг придёт NACK на ремонт
            t.lastRxMs = now;
        }
    }

    private void handleNack(SocketAddress peer, Packet nack) throws Exception {
        Transfer t = current;
        if (t == null) return;
        if (!Objects.equals(t.peer, peer)) return;
        if (t.transferId != nack.transferId) return;

        t.touchRx();

        // payload = ranges: [start(int), len(int)] * N
        ByteBuffer bb = ByteBuffer.wrap(nack.payload);
        int ranges = Math.min(bb.remaining() / 8, MAX_NACK_RANGES);

        int resent = 0;
        for (int i = 0; i < ranges; i++) {
            int start = bb.getInt();
            int len = bb.getInt();
            if (len <= 0) continue;

            int end = Math.min(t.totalChunks, start + len);
            for (int seq = Math.max(0, start); seq < end; seq++) {
                // resend missing
                long off = (long) seq * t.chunk;
                int plen = (int) Math.min(t.chunk, t.totalBytes - off);
                byte[] data = new byte[plen];
                t.raf.seek(off);
                t.raf.readFully(data);
                sendPacket(t.peer, T_DATA, t.transferId, seq, t.totalChunks, t.chunk, data);
                resent++;
            }
        }

        // После дозапросов — снова DONE (пусть клиент решит, всё ли ок)
        sendPacket(t.peer, T_DONE, t.transferId, 0, t.totalChunks, t.chunk, "DONE".getBytes(StandardCharsets.UTF_8));
    }

    private void handleDone(SocketAddress peer, Packet done) throws Exception {
        Transfer t = current;
        if (t == null) return;
        if (!Objects.equals(t.peer, peer)) return;
        if (t.transferId != done.transferId) return;

        // Клиент сказал "хватит"
        t.closeQuiet();
        current = null;
    }

    // ===== PUT (upload) =====
    private void handlePut(SocketAddress peer, Packet req) throws Exception {
        // payload: "name\nsize\nchunk\n"
        String[] lines = req.payloadText().split("\n");
        if (lines.length < 3) {
            sendErr(peer, 0, "Bad PUT");
            return;
        }
        String name = sanitizeFileName(lines[0].trim());
        long total = parseLong(lines[1].trim(), -1);
        int chunk = parseInt(lines[2].trim(), MAX_PAYLOAD);
        if (name == null || total < 0 || chunk <= 0 || chunk > MAX_PAYLOAD) {
            sendErr(peer, 0, "Bad args");
            return;
        }

        File f = new File(baseDir, name);
        ensureDir(f.getParentFile());

        int totalChunks = (int) ((total + chunk - 1L) / chunk);
        int tid = rnd.nextInt();
        RandomAccessFile raf = new RandomAccessFile(f, "rw");
        raf.setLength(total);

        Transfer t = new Transfer(true, peer, tid, f, raf, total, chunk, totalChunks);
        current = t;

        String info = "OK " + name + " " + total + "\n";
        sendPacket(peer, T_INFO, tid, 0, totalChunks, chunk, info.getBytes(StandardCharsets.UTF_8));
        t.touchRx();
    }

    private void handleData(SocketAddress peer, Packet data) throws Exception {
        Transfer t = current;
        if (t == null) return;
        if (!t.upload) return; // server receives DATA only for upload
        if (!Objects.equals(t.peer, peer)) return;
        if (t.transferId != data.transferId) return;

        t.touchRx();
        int seq = data.seq;
        if (seq < 0 || seq >= t.totalChunks) return;

        if (!t.received.get(seq)) {
            long off = (long) seq * t.chunk;
            int expected = (int) Math.min(t.chunk, t.totalBytes - off);
            if (data.payload.length != expected) return;

            t.raf.seek(off);
            t.raf.write(data.payload);
            t.received.set(seq);
        }

        // НИКАКИХ ACK на каждый пакет.
        // Только если клиент сам запросит ремонт — тогда NACK’и будет слать сервер (но мы здесь держим максимально просто).
        // Клиент сам завершит, когда решит.
    }

    // ====== IO ======
    private Packet recvPacket(DatagramPacket dp) throws IOException {
        try {
            sock.receive(dp);
        } catch (SocketTimeoutException e) {
            return null;
        }
        ByteBuffer bb = ByteBuffer.wrap(dp.getData(), dp.getOffset(), dp.getLength());
        if (bb.remaining() < HDR) return null;

        short magic = bb.getShort();
        byte ver = bb.get();
        byte type = bb.get();
        if (magic != MAGIC || ver != VER) return null;

        int tid = bb.getInt();
        int seq = bb.getInt();
        int totalChunks = bb.getInt();
        int chunk = Short.toUnsignedInt(bb.getShort());
        int payloadLen = Short.toUnsignedInt(bb.getShort());
        if (payloadLen > bb.remaining()) return null;

        byte[] payload = new byte[payloadLen];
        bb.get(payload);

        return new Packet(type, tid, seq, totalChunks, chunk, payload);
    }

    private void sendErr(SocketAddress peer, int tid, String msg) throws IOException {
        sendPacket(peer, T_ERR, tid, 0, 0, 0, ("ERR " + msg).getBytes(StandardCharsets.UTF_8));
    }

    private void sendPacket(SocketAddress peer, byte type, int tid, int seq, int totalChunks, int chunk, byte[] payload) throws IOException {
        ByteBuffer bb = ByteBuffer.allocate(HDR + payload.length);
        bb.putShort(MAGIC);
        bb.put(VER);
        bb.put(type);
        bb.putInt(tid);
        bb.putInt(seq);
        bb.putInt(totalChunks);
        bb.putShort((short) chunk);
        bb.putShort((short) payload.length);
        bb.put(payload);

        DatagramPacket dp = new DatagramPacket(bb.array(), bb.position());
        dp.setSocketAddress(peer);
        sock.send(dp);
    }

    // ====== utils ======
    private static void ensureDir(File dir) throws IOException {
        if (dir == null) return;
        if (!dir.exists() && !dir.mkdirs()) throw new IOException("Cannot create dir: " + dir);
    }

    private static String sanitizeFileName(String raw) {
        if (raw == null || raw.isBlank()) return null;
        raw = raw.replace("\\", "/");
        if (raw.contains("..") || raw.startsWith("/") || raw.contains("/")) return null;
        return raw;
    }

    private static long parseLong(String s, long def) {
        try { return Long.parseLong(s.trim()); } catch (Exception e) { return def; }
    }

    private static int parseInt(String s, int def) {
        try { return Integer.parseInt(s.trim()); } catch (Exception e) { return def; }
    }

    public static void main(String[] args) throws Exception {
        int port = args.length > 0 ? Integer.parseInt(args[0]) : PORT_DEFAULT;
        File dir = new File(args.length > 1 ? args[1] : DIR_DEFAULT);
        new UdpFastFileServer(port, dir).run();
    }
}
