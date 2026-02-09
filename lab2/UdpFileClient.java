package org.example;

import java.io.*;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;

public final class UdpFileClient {

    // ===== defaults =====
    private static final int DEFAULT_PORT = 50506;

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

    private static final int HDR = 34;
    private static final int MAX_UDP = 65507;
    private static final int MAX_PAYLOAD = MAX_UDP - HDR;

    private static final int ACK_BITMAP_BITS = 32;

    // retransmit
    private static final int DEFAULT_WINDOW = 64;        // скользящее окно
    private static final int DEFAULT_RTO_MS = 120;        // таймаут повтора (LAN)
    private static final int DEFAULT_MAX_RETRY = 50;      // чтобы не зависнуть навсегда

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            usage();
            return;
        }
        String host = args[0];
        int port = Integer.parseInt(args[1]);
        String cmd = args.length >= 3 ? args[2].toLowerCase(Locale.ROOT) : "";

        switch (cmd) {
            case "time" -> runCmd(host, port, "TIME");
            case "echo" -> runCmd(host, port, "ECHO " + joinFrom(args, 3));
            case "close" -> runCmd(host, port, "CLOSE");
            case "upload" -> {
                if (args.length < 5) { usage(); return; }
                File local = new File(args[3]);
                String remote = args[4];
                int chunk = args.length >= 6 ? Integer.parseInt(args[5]) : 1200;
                int window = args.length >= 7 ? Integer.parseInt(args[6]) : DEFAULT_WINDOW;
                upload(host, port, local, remote, chunk, window);
            }
            case "download" -> {
                if (args.length < 5) { usage(); return; }
                String remote = args[3];
                File local = new File(args[4]);
                int chunk = args.length >= 6 ? Integer.parseInt(args[5]) : 1200;
                int window = args.length >= 7 ? Integer.parseInt(args[6]) : DEFAULT_WINDOW;
                download(host, port, remote, local, chunk, window);
            }
            case "bench-upload" -> {
                if (args.length < 5) { usage(); return; }
                File local = new File(args[3]);
                String remote = args[4];
                benchUpload(host, port, local, remote);
            }
            default -> usage();
        }
    }

    private static void usage() {
        System.out.println("""
        Usage (UDP):
          java org.example.UdpFileClient <host> <port> time
          java org.example.UdpFileClient <host> <port> echo <text...>
          java org.example.UdpFileClient <host> <port> close

          java org.example.UdpFileClient <host> <port> upload   <localPath> <remoteName> [chunk=1200] [window=64]
          java org.example.UdpFileClient <host> <port> download <remoteName> <localPath> [chunk=1200] [window=64]

          java org.example.UdpFileClient <host> <port> bench-upload <localPath> <remoteName>

        Notes:
          chunk — размер полезной нагрузки UDP (байт), <= %d
          window — размер скользящего окна (кол-во пакетов "в полёте")
        """.formatted(MAX_PAYLOAD));
    }

    // ===== session =====
    private static final class Session implements Closeable {
        final DatagramSocket sock;
        final InetSocketAddress server;
        final long sessionId;

        Session(InetSocketAddress server) throws SocketException {
            this.sock = new DatagramSocket();
            this.server = server;
            this.sessionId = new Random().nextLong();
        }

        @Override public void close() { sock.close(); }
    }

    // ===== commands =====
    private static void runCmd(String host, int port, String line) throws Exception {
        InetSocketAddress server = new InetSocketAddress(host, port);
        try (Session s = new Session(server)) {
            send(s.sock, s.server, T_CMD, s.sessionId, 0, 0, 0, 0, line.getBytes(StandardCharsets.UTF_8));
            Packet resp = recv(s.sock, 2000);
            if (resp == null) {
                System.out.println("No response (timeout).");
                return;
            }
            System.out.println(resp.payloadText());
        }
    }

    // ===== upload =====
    private static void upload(String host, int port, File local, String remote, int chunk, int window) throws Exception {
        if (!local.exists() || !local.isFile()) throw new FileNotFoundException(local.getPath());
        if (chunk <= 0 || chunk > MAX_PAYLOAD) throw new IllegalArgumentException("Bad chunk: " + chunk);
        if (window <= 0) throw new IllegalArgumentException("Bad window: " + window);

        InetSocketAddress server = new InetSocketAddress(host, port);
        try (Session s = new Session(server)) {
            long total = local.length();
            String meta = remote + "\n" + total + "\n" + chunk + "\n";
            send(s.sock, s.server, T_START_UPLOAD, s.sessionId, 0, 0, 0, 0, meta.getBytes(StandardCharsets.UTF_8));

            Packet startResp = recv(s.sock, 3000);
            if (startResp == null) throw new IOException("START_UPLOAD: no response");
            if (startResp.type == T_ERR) throw new IOException(startResp.payloadText());
            String[] ok = startResp.payloadText().split("\\s+");
            if (ok.length < 5 || !ok[0].equals("OK")) throw new IOException("Bad START_UPLOAD response: " + startResp.payloadText());

            int transferId = Integer.parseInt(ok[1]);
            int totalChunks = Integer.parseInt(ok[4]);

            System.out.println("UPLOAD: transferId=" + transferId + " total=" + total + " chunk=" + chunk + " chunks=" + totalChunks + " window=" + window);

            // sender state
            BitSet acked = new BitSet(totalChunks);
            long[] sentAt = new long[totalChunks];
            int[] retries = new int[totalChunks];

            long startNs = System.nanoTime();

            try (RandomAccessFile raf = new RandomAccessFile(local, "r")) {
                int base = 0;
                int next = 0;

                // основной цикл пока не подтверждены все чанки
                while (true) {
                    // слать пока окно позволяет
                    while (next < totalChunks && next < base + window) {
                        if (!acked.get(next)) {
                            byte[] data = readChunk(raf, total, chunk, next);
                            send(s.sock, s.server, T_DATA, s.sessionId, transferId, next, 0, 0, data);
                            sentAt[next] = System.currentTimeMillis();
                        }
                        next++;
                    }

                    // ждать ACK/RESP
                    Packet pkt = recv(s.sock, DEFAULT_RTO_MS);
                    long now = System.currentTimeMillis();

                    if (pkt != null) {
                        if (pkt.type == T_ACK && pkt.transferId == transferId) {
                            // применяем ACK + SACK
                            applyAck(acked, pkt.ackBase, pkt.sack, totalChunks);

                            // двигаем base
                            base = acked.nextClearBit(0);
                            if (base >= totalChunks) break; // done
                            if (next < base) next = base;
                        } else if (pkt.type == T_ERR) {
                            throw new IOException(pkt.payloadText());
                        }
                    }

                    // ретрансмит по таймауту внутри окна
                    int end = Math.min(totalChunks, base + window);
                    for (int i = base; i < end; i++) {
                        if (acked.get(i)) continue;
                        if (sentAt[i] == 0) continue;

                        if (now - sentAt[i] > DEFAULT_RTO_MS) {
                            if (retries[i]++ > DEFAULT_MAX_RETRY) {
                                throw new IOException("Too many retries at seq=" + i + " (likely DROP / link down)");
                            }
                            byte[] data = readChunk(raf, total, chunk, i);
                            send(s.sock, s.server, T_DATA, s.sessionId, transferId, i, 0, 0, data);
                            sentAt[i] = now;
                        }
                    }
                }
            }

            // финализация
            send(s.sock, s.server, T_FIN, s.sessionId, transferId, 0, 0, 0, new byte[0]);
            Packet fin = recv(s.sock, 2000);
            if (fin == null) throw new IOException("No FIN response");
            System.out.println(fin.payloadText());

            printBitrate("UPLOAD bitrate", total, startNs);
        }
    }

    // ===== download =====
    private static void download(String host, int port, String remote, File local, int chunk, int window) throws Exception {
        if (chunk <= 0 || chunk > MAX_PAYLOAD) throw new IllegalArgumentException("Bad chunk: " + chunk);
        if (window <= 0) throw new IllegalArgumentException("Bad window: " + window);

        InetSocketAddress server = new InetSocketAddress(host, port);
        try (Session s = new Session(server)) {
            String meta = remote + "\n" + chunk + "\n";
            send(s.sock, s.server, T_START_DOWNLOAD, s.sessionId, 0, 0, 0, 0, meta.getBytes(StandardCharsets.UTF_8));

            Packet startResp = recv(s.sock, 3000);
            if (startResp == null) throw new IOException("START_DOWNLOAD: no response");
            if (startResp.type == T_ERR) throw new IOException(startResp.payloadText());
            String[] ok = startResp.payloadText().split("\\s+");
            if (ok.length < 5 || !ok[0].equals("OK")) throw new IOException("Bad START_DOWNLOAD response: " + startResp.payloadText());

            int transferId = Integer.parseInt(ok[1]);
            long total = Long.parseLong(ok[2]);
            int totalChunks = Integer.parseInt(ok[4]);

            System.out.println("DOWNLOAD: transferId=" + transferId + " total=" + total + " chunk=" + chunk + " chunks=" + totalChunks + " window=" + window);

            BitSet received = new BitSet(totalChunks);
            long startNs = System.nanoTime();

            try (RandomAccessFile raf = new RandomAccessFile(local, "rw")) {
                raf.setLength(total);

                int base = 0;
                long lastAckSent = 0;

                while (base < totalChunks) {
                    Packet pkt = recv(s.sock, 2000);
                    if (pkt == null) {
                        // если REJECT/DROP/обрыв — просто перекидываем ACK (сервер дослёт)
                        sendAck(s, transferId, received, totalChunks);
                        continue;
                    }
                    if (pkt.type == T_DATA && pkt.transferId == transferId) {
                        int seq = pkt.seq;
                        if (seq >= 0 && seq < totalChunks) {
                            if (!received.get(seq)) {
                                long offset = (long) seq * chunk;
                                raf.seek(offset);
                                raf.write(pkt.payload);
                                received.set(seq);
                            }
                            base = received.nextClearBit(0);
                            // ACK не на каждый пакет (чтобы не забить канал): раз в ~20мс или при продвижении base
                            long now = System.currentTimeMillis();
                            if (now - lastAckSent > 20 || base == seq) {
                                sendAck(s, transferId, received, totalChunks);
                                lastAckSent = now;
                            }
                        }
                    } else if (pkt.type == T_ERR) {
                        throw new IOException(pkt.payloadText());
                    }
                }

                // подтверждаем финал
                send(s.sock, s.server, T_FIN, s.sessionId, transferId, 0, 0, 0, new byte[0]);

                Packet finResp = null;
                long deadline = System.currentTimeMillis() + 2000;

                while (System.currentTimeMillis() < deadline) {
                    Packet pkt = recv(s.sock, 300);
                    if (pkt == null) continue;

                    // Игнорируем запоздавшие DATA
                    if (pkt.type == T_DATA) continue;

                    // Берём только текстовый ответ
                    if ((pkt.type == T_CMD_RESP || pkt.type == T_ERR) && pkt.transferId == transferId) {
                        finResp = pkt;
                        break;
                    }
                }

                if (finResp != null) System.out.println(finResp.payloadText());
                else System.out.println("FIN response: timeout (maybe last DATA arrived late)");
            }

            printBitrate("DOWNLOAD bitrate", total, startNs);
        }
    }

    private static void sendAck(Session s, int transferId, BitSet received, int totalChunks) throws IOException {
        int base = received.nextClearBit(0);
        int sack = 0;
        for (int b = 0; b < ACK_BITMAP_BITS; b++) {
            int idx = base + 1 + b;
            if (idx >= 0 && idx < totalChunks && received.get(idx)) sack |= (1 << b);
        }
        send(s.sock, s.server, T_ACK, s.sessionId, transferId, 0, base, sack, new byte[0]);
    }

    // ===== bench =====
    private static void benchUpload(String host, int port, File local, String remote) throws Exception {
        int[] chunks = {256, 512, 768, 1024, 1200, 1400, 1472, 2048, 4096, 8192};
        int[] windows = {16, 32, 64, 128};

        System.out.println("BENCH UPLOAD: " + local.getName() + " size=" + local.length());
        System.out.println("Server: " + host + ":" + port);
        System.out.println();

        for (int w : windows) {
            for (int c : chunks) {
                if (c > MAX_PAYLOAD) continue;
                // чтобы не перезаписывать одно и то же имя:
                String name = remote + ".c" + c + ".w" + w;

                long t0 = System.nanoTime();
                try {
                    upload(host, port, local, name, c, w);
                    double sec = (System.nanoTime() - t0) / 1_000_000_000.0;
                    double mbit = (local.length() * 8.0) / 1_000_000.0;
                    double mbps = mbit / sec;
                    System.out.printf("RESULT: chunk=%-5d window=%-4d => %.2f Mbit/s%n", c, w, mbps);
                } catch (Exception e) {
                    System.out.printf("RESULT: chunk=%-5d window=%-4d => FAIL (%s)%n", c, w, e.getMessage());
                }
                System.out.println();
            }
        }
    }

    // ===== protocol packet =====
    private static final class Packet {
        final byte type;
        final long sessionId;
        final int transferId;
        final int seq;
        final int ackBase;
        final int sack;
        final byte[] payload;

        Packet(byte type, long sessionId, int transferId, int seq, int ackBase, int sack, byte[] payload) {
            this.type = type;
            this.sessionId = sessionId;
            this.transferId = transferId;
            this.seq = seq;
            this.ackBase = ackBase;
            this.sack = sack;
            this.payload = payload;
        }

        String payloadText() {
            return new String(payload, StandardCharsets.UTF_8).trim();
        }
    }

    private static Packet recv(DatagramSocket sock, int timeoutMs) throws IOException {
        sock.setSoTimeout(timeoutMs);
        byte[] buf = new byte[MAX_UDP];
        DatagramPacket p = new DatagramPacket(buf, buf.length);
        try {
            sock.receive(p);
        } catch (SocketTimeoutException e) {
            return null;
        }

        ByteBuffer bb = ByteBuffer.wrap(p.getData(), p.getOffset(), p.getLength());
        if (bb.remaining() < HDR) return null;

        short magic = bb.getShort();
        byte ver = bb.get();
        byte type = bb.get();
        if (magic != MAGIC || ver != VER) return null;

        long sessionId = bb.getLong();
        int transferId = bb.getInt();
        int seq = bb.getInt();
        int ackBase = bb.getInt();
        int sack = bb.getInt();
        int payloadLen = Short.toUnsignedInt(bb.getShort());
        if (payloadLen < 0 || payloadLen > bb.remaining()) return null;
        byte[] payload = new byte[payloadLen];
        bb.get(payload);

        return new Packet(type, sessionId, transferId, seq, ackBase, sack, payload);
    }

    private static void send(DatagramSocket sock, InetSocketAddress server, byte type,
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
        DatagramPacket dp = new DatagramPacket(out, out.length, server);
        sock.send(dp);
    }

    private static void applyAck(BitSet acked, int ackBase, int sack, int totalChunks) {
        // все < ackBase подтверждены
        for (int i = 0; i < ackBase && i < totalChunks; i++) acked.set(i);

        // bitmap: ackBase+1..ackBase+32
        for (int b = 0; b < ACK_BITMAP_BITS; b++) {
            if (((sack >>> b) & 1) != 0) {
                int idx = ackBase + 1 + b;
                if (idx >= 0 && idx < totalChunks) acked.set(idx);
            }
        }
    }

    private static byte[] readChunk(RandomAccessFile raf, long total, int chunk, int seq) throws IOException {
        long offset = (long) seq * chunk;
        int len = (int) Math.min(chunk, total - offset);
        byte[] data = new byte[len];
        raf.seek(offset);
        raf.readFully(data);
        return data;
    }

    private static void printBitrate(String label, long bytes, long startNs) {
        double sec = (System.nanoTime() - startNs) / 1_000_000_000.0;
        if (sec <= 0.000001) return;
        double mbit = (bytes * 8.0) / 1_000_000.0;
        System.out.printf("%s: %.2f Mbit/s (%d bytes in %.3f s)%n", label, mbit / sec, bytes, sec);
    }

    private static String joinFrom(String[] args, int idx) {
        if (idx >= args.length) return "";
        StringBuilder sb = new StringBuilder();
        for (int i = idx; i < args.length; i++) {
            if (i > idx) sb.append(' ');
            sb.append(args[i]);
        }
        return sb.toString();
    }
}
