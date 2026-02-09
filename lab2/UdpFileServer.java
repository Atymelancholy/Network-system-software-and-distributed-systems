package org.example;

import java.io.*;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public final class UdpFileServer {

    // ===== defaults =====
    private static final int DEFAULT_PORT = 50506;
    private static final String DEFAULT_DIR = "server_storage_udp";

    // протокол
    private static final short MAGIC = (short) 0x5546; // 'U''F'
    private static final byte VER = 1;

    // types
    private static final byte T_CMD = 1;
    private static final byte T_CMD_RESP = 2;
    private static final byte T_START_UPLOAD = 3;
    private static final byte T_START_DOWNLOAD = 4;
    private static final byte T_DATA = 5;
    private static final byte T_ACK = 6;
    private static final byte T_FIN = 7;
    private static final byte T_ERR = 8;

    // header: 34 bytes
    // magic(2) ver(1) type(1) sessionId(8) transferId(4) seq(4) ackBase(4) sack(4) payloadLen(2) = 34
    private static final int HDR = 34;

    // sane limits
    private static final int MAX_UDP = 65507;
    private static final int MAX_PAYLOAD = MAX_UDP - HDR;

    // retransmit/ack behavior (receiver)
    private static final int ACK_BITMAP_BITS = 32;
    private static final long STATE_TTL_MS = 180_000; // чистим старые сессии (3 мин)

    private final int port;
    private final File baseDir;

    public UdpFileServer(int port, File baseDir) {
        this.port = port;
        this.baseDir = baseDir;
    }

    public void start() throws IOException {
        ensureDir(baseDir);

        try (DatagramSocket sock = new DatagramSocket(port)) {
            sock.setSoTimeout(1000); // чтобы периодически чистить старые состояния
            System.out.println("UDP server started");
            System.out.println("Port: " + port);
            System.out.println("Storage: " + baseDir.getAbsolutePath());
            System.out.println("Now: " + Instant.now());
            System.out.println("\nWaiting for UDP datagrams...\n");

            byte[] buf = new byte[MAX_UDP];
            DatagramPacket p = new DatagramPacket(buf, buf.length);

            while (true) {
                try {
                    sock.receive(p);
                    handlePacket(sock, p);
                } catch (SocketTimeoutException ignore) {
                    cleanupOldStates();
                } catch (Exception e) {
                    System.out.println("Server loop error: " + e.getMessage());
                }
            }
        }
    }

    // ===== session state =====
    private static final class Key {
        final SocketAddress addr;
        final long sessionId;
        final int transferId;

        Key(SocketAddress addr, long sessionId, int transferId) {
            this.addr = addr;
            this.sessionId = sessionId;
            this.transferId = transferId;
        }
        @Override public boolean equals(Object o) {
            if (!(o instanceof Key k)) return false;
            return sessionId == k.sessionId && transferId == k.transferId && Objects.equals(addr, k.addr);
        }
        @Override public int hashCode() {
            return Objects.hash(addr, sessionId, transferId);
        }
    }

    private static final class TransferState {
        final boolean upload; // true: client->server, false: server->client (download)
        final File file;
        final RandomAccessFile raf;
        final long totalBytes;
        final int chunkSize;
        final int totalChunks;
        final BitSet received; // для upload или для download ack tracking
        long lastTouchMs = System.currentTimeMillis();

        // bitrate
        long startedNs = System.nanoTime();
        long bytesDone = 0;

        TransferState(boolean upload, File file, RandomAccessFile raf,
                      long totalBytes, int chunkSize, int totalChunks) {
            this.upload = upload;
            this.file = file;
            this.raf = raf;
            this.totalBytes = totalBytes;
            this.chunkSize = chunkSize;
            this.totalChunks = totalChunks;
            this.received = new BitSet(totalChunks);
        }

        void touch() { lastTouchMs = System.currentTimeMillis(); }
        void closeQuiet() { try { raf.close(); } catch (Exception ignored) {} }
    }

    private final Map<Key, TransferState> states = new ConcurrentHashMap<>();
    private final Random rnd = new Random();

    private void cleanupOldStates() {
        long now = System.currentTimeMillis();
        for (var it = states.entrySet().iterator(); it.hasNext(); ) {
            var e = it.next();
            if (now - e.getValue().lastTouchMs > STATE_TTL_MS) {
                e.getValue().closeQuiet();
                it.remove();
            }
        }
    }

    // ===== packet handling =====
    private void handlePacket(DatagramSocket sock, DatagramPacket p) throws IOException {
        ByteBuffer bb = ByteBuffer.wrap(p.getData(), p.getOffset(), p.getLength());
        if (bb.remaining() < HDR) return;

        short magic = bb.getShort();
        byte ver = bb.get();
        byte type = bb.get();
        if (magic != MAGIC || ver != VER) return;

        long sessionId = bb.getLong();
        int transferId = bb.getInt();
        int seq = bb.getInt();
        int ackBase = bb.getInt();
        int sack = bb.getInt();
        int payloadLen = Short.toUnsignedInt(bb.getShort());
        if (payloadLen < 0 || payloadLen > bb.remaining()) return;

        byte[] payload = new byte[payloadLen];
        bb.get(payload);

        SocketAddress peer = p.getSocketAddress();

        switch (type) {
            case T_CMD -> handleCmd(sock, peer, sessionId, payload);
            case T_START_UPLOAD -> handleStartUpload(sock, peer, sessionId, payload);
            case T_START_DOWNLOAD -> handleStartDownload(sock, peer, sessionId, payload);
            case T_DATA -> handleData(sock, peer, sessionId, transferId, seq, payload);
            case T_ACK -> handleAck(sock, peer, sessionId, transferId, ackBase, sack);
            case T_FIN -> handleFin(sock, peer, sessionId, transferId);
            default -> { /* ignore */ }
        }
    }

    private void handleCmd(DatagramSocket sock, SocketAddress peer, long sessionId, byte[] payload) throws IOException {
        String cmdLine = new String(payload, StandardCharsets.UTF_8).trim();
        String[] parts = cmdLine.split("\\s+");
        String cmd = parts[0].toUpperCase(Locale.ROOT);

        String resp;
        switch (cmd) {
            case "TIME" -> resp = "OK " + Instant.now();
            case "ECHO" -> {
                String msg = cmdLine.length() > 4 ? cmdLine.substring(4).trim() : "";
                resp = "OK " + msg;
            }
            case "CLOSE", "EXIT", "QUIT" -> resp = "BYE";
            default -> resp = "ERR Unknown command";
        }
        send(sock, peer, T_CMD_RESP, sessionId, 0, 0, 0, 0, resp.getBytes(StandardCharsets.UTF_8));
    }

    // START_UPLOAD payload:
    // name\nsize\nchunkSize\n
    private void handleStartUpload(DatagramSocket sock, SocketAddress peer, long sessionId, byte[] payload) throws IOException {
        String s = new String(payload, StandardCharsets.UTF_8);
        String[] lines = s.split("\n");
        if (lines.length < 3) {
            sendErr(sock, peer, sessionId, "Bad START_UPLOAD");
            return;
        }
        String rawName = lines[0].trim();
        long total = parseLong(lines[1].trim(), -1);
        int chunk = (int) parseLong(lines[2].trim(), -1);

        String name = sanitizeFileName(rawName);
        if (name == null || total < 0 || chunk <= 0 || chunk > MAX_PAYLOAD) {
            sendErr(sock, peer, sessionId, "Bad args (name/size/chunk)");
            return;
        }

        int totalChunks = (int) ((total + chunk - 1L) / chunk);
        int transferId = rnd.nextInt();

        File target = new File(baseDir, name);
        ensureDir(target.getParentFile());

        RandomAccessFile raf = new RandomAccessFile(target, "rw");
        raf.setLength(total);

        TransferState st = new TransferState(true, target, raf, total, chunk, totalChunks);
        Key key = new Key(peer, sessionId, transferId);
        states.put(key, st);

        String resp = "OK " + transferId + " " + total + " " + chunk + " " + totalChunks;
        send(sock, peer, T_CMD_RESP, sessionId, transferId, 0, 0, 0, resp.getBytes(StandardCharsets.UTF_8));
    }

    // START_DOWNLOAD payload:
    // name\nchunkSize\n
    private void handleStartDownload(DatagramSocket sock, SocketAddress peer, long sessionId, byte[] payload) throws IOException {
        String s = new String(payload, StandardCharsets.UTF_8);
        String[] lines = s.split("\n");
        if (lines.length < 2) {
            sendErr(sock, peer, sessionId, "Bad START_DOWNLOAD");
            return;
        }

        String name = sanitizeFileName(lines[0].trim());
        int chunk = (int) parseLong(lines[1].trim(), -1);

        if (name == null || chunk <= 0 || chunk > MAX_PAYLOAD) {
            sendErr(sock, peer, sessionId, "Bad args (name/chunk)");
            return;
        }

        File source = new File(baseDir, name);
        if (!source.exists() || !source.isFile()) {
            sendErr(sock, peer, sessionId, "No such file");
            return;
        }

        long total = source.length();
        int totalChunks = (int) ((total + chunk - 1L) / chunk);
        int transferId = rnd.nextInt();

        RandomAccessFile raf = new RandomAccessFile(source, "r");
        TransferState st = new TransferState(false, source, raf, total, chunk, totalChunks);
        Key key = new Key(peer, sessionId, transferId);
        states.put(key, st);

        String resp = "OK " + transferId + " " + total + " " + chunk + " " + totalChunks;
        send(sock, peer, T_CMD_RESP, sessionId, transferId, 0, 0, 0, resp.getBytes(StandardCharsets.UTF_8));

        // стартуем «пуш» данных: сервер будет слать DATA, а клиент слать ACK
        // (управление окном — на стороне сервера в handleAck)
        sendMoreDownloadData(sock, peer, sessionId, transferId, st, 0, 64); // initial burst (window=64)
    }

    private void handleData(DatagramSocket sock, SocketAddress peer, long sessionId, int transferId, int seq, byte[] payload) throws IOException {
        Key key = new Key(peer, sessionId, transferId);
        TransferState st = states.get(key);
        if (st == null || !st.upload) return;

        st.touch();
        if (seq < 0 || seq >= st.totalChunks) return;

        // write chunk to file offset
        long offset = (long) seq * st.chunkSize;
        int expectedLen = (int) Math.min(st.chunkSize, st.totalBytes - offset);
        if (payload.length != expectedLen) {
            // допускаем последний пакет короче (но ожидаем именно expectedLen)
            return;
        }

        // дедуп
        if (!st.received.get(seq)) {
            st.raf.seek(offset);
            st.raf.write(payload);
            st.received.set(seq);
            st.bytesDone += payload.length;
        }

        // отвечаем ACK (c SACK bitmap)
        sendAck(sock, peer, sessionId, transferId, st);
    }

    private void handleAck(DatagramSocket sock, SocketAddress peer, long sessionId, int transferId, int ackBase, int sack) throws IOException {
        Key key = new Key(peer, sessionId, transferId);
        TransferState st = states.get(key);
        if (st == null || st.upload) return; // ACK интересует только download (server->client)

        st.touch();

        // помечаем подтверждённые чанки
        // ackBase означает: все < ackBase получены
        for (int i = 0; i < ackBase && i < st.totalChunks; i++) st.received.set(i);

        // bitmap подтверждает следующие 32 после ackBase
        for (int b = 0; b < ACK_BITMAP_BITS; b++) {
            if (((sack >>> b) & 1) != 0) {
                int idx = ackBase + 1 + b;
                if (idx >= 0 && idx < st.totalChunks) st.received.set(idx);
            }
        }

        // досылаем новые данные по окну
        int base = st.received.nextClearBit(0);
        int window = 64; // можно сделать параметром
        sendMoreDownloadData(sock, peer, sessionId, transferId, st, base, window);
    }

    private void handleFin(DatagramSocket sock, SocketAddress peer, long sessionId, int transferId) throws IOException {
        Key key = new Key(peer, sessionId, transferId);
        TransferState st = states.get(key);
        if (st == null) return;

        st.touch();

        boolean done = st.received.nextClearBit(0) >= st.totalChunks;
        if (done) {
            double sec = (System.nanoTime() - st.startedNs) / 1_000_000_000.0;
            double mbit = (st.bytesDone * 8.0) / 1_000_000.0;
            System.out.printf("%s DONE: %.2f Mbit/s (%d bytes in %.3f s) file=%s%n",
                    st.upload ? "UPLOAD" : "DOWNLOAD", (sec > 0 ? (mbit / sec) : 0), st.bytesDone, sec, st.file.getName());

            send(sock, peer, T_CMD_RESP, sessionId, transferId, 0, 0, 0,
                    "OK DONE".getBytes(StandardCharsets.UTF_8));
            st.closeQuiet();
            states.remove(key);
        } else {
            send(sock, peer, T_CMD_RESP, sessionId, transferId, 0, 0, 0,
                    "ERR NOT_DONE".getBytes(StandardCharsets.UTF_8));
        }
    }

    // ===== download sending =====
    private void sendMoreDownloadData(DatagramSocket sock, SocketAddress peer, long sessionId, int transferId,
                                      TransferState st, int base, int window) throws IOException {
        if (st == null) return;

        int sent = 0;
        for (int seq = base; seq < st.totalChunks && seq < base + window; seq++) {
            if (st.received.get(seq)) continue;

            long offset = (long) seq * st.chunkSize;
            int len = (int) Math.min(st.chunkSize, st.totalBytes - offset);
            byte[] data = new byte[len];

            st.raf.seek(offset);
            st.raf.readFully(data);

            send(sock, peer, T_DATA, sessionId, transferId, seq, 0, 0, data);
            st.bytesDone += len;
            sent++;
        }
        if (sent > 0) st.touch();
    }

    // ===== ack builder (for uploads) =====
    private void sendAck(DatagramSocket sock, SocketAddress peer, long sessionId, int transferId, TransferState st) throws IOException {
        int base = st.received.nextClearBit(0);
        int bitmap = 0;
        for (int b = 0; b < ACK_BITMAP_BITS; b++) {
            int idx = base + 1 + b;
            if (idx >= 0 && idx < st.totalChunks && st.received.get(idx)) {
                bitmap |= (1 << b);
            }
        }
        send(sock, peer, T_ACK, sessionId, transferId, 0, base, bitmap, new byte[0]);
    }

    // ===== send helpers =====
    private void sendErr(DatagramSocket sock, SocketAddress peer, long sessionId, String msg) throws IOException {
        send(sock, peer, T_ERR, sessionId, 0, 0, 0, 0, ("ERR " + msg).getBytes(StandardCharsets.UTF_8));
    }

    private static void send(DatagramSocket sock, SocketAddress peer, byte type,
                             long sessionId, int transferId, int seq, int ackBase, int sack, byte[] payload) throws IOException {
        if (payload.length > MAX_PAYLOAD) throw new IOException("Payload too large: " + payload.length);

        ByteBuffer bb = ByteBuffer.allocate(HDR + payload.length);
        bb.putShort(MAGIC);
        bb.put(VER);
        bb.put(type);
        bb.putLong(sessionId);
        bb.putInt(transferId);
        bb.putInt(seq);
        bb.putInt(ackBase);
        bb.putInt(sack);
        bb.putShort((short) payload.length);
        bb.put(payload);

        byte[] out = bb.array();
        DatagramPacket dp = new DatagramPacket(out, out.length);
        dp.setSocketAddress(peer);
        sock.send(dp);
    }

    // ===== utils =====
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
        try { return Long.parseLong(s); } catch (Exception e) { return def; }
    }

    public static void main(String[] args) throws Exception {
        int port = args.length > 0 ? Integer.parseInt(args[0]) : DEFAULT_PORT;
        File dir = new File(args.length > 1 ? args[1] : DEFAULT_DIR);
        new UdpFileServer(port, dir).start();
    }
}
