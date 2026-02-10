package org.example;

import java.io.*;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;

public final class UdpFastFileClient {

    // ====== TUNING ======
    private static final int MAX_PAYLOAD = 1472;

    // Сколько раз делаем repair (NACK -> resend)
    private static final int REPAIR_ROUNDS = 3;

    // Сколько ждём “тишины”, чтобы считать blast законченным
    private static final int IDLE_FINISH_MS = 120;

    // Сколько ranges максимум в одном NACK
    private static final int MAX_NACK_RANGES = 2048;

    // socket buffers
    private static final int SO_RCVBUF = 16 * 1024 * 1024;
    private static final int SO_SNDBUF = 8 * 1024 * 1024;

    // ====== PROTOCOL ======
    private static final short MAGIC = (short) 0x5546;
    private static final byte VER = 1;

    private static final byte T_REQ_GET  = 1;
    private static final byte T_REQ_PUT  = 2;
    private static final byte T_INFO     = 3;
    private static final byte T_DATA     = 4;
    private static final byte T_NACK     = 5;
    private static final byte T_DONE     = 6;
    private static final byte T_ERR      = 7;

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

    private final DatagramSocket sock;
    private final InetSocketAddress server;

    public UdpFastFileClient(InetSocketAddress server) throws Exception {
        this.server = server;
        this.sock = new DatagramSocket();
        this.sock.setReceiveBufferSize(SO_RCVBUF);
        this.sock.setSendBufferSize(SO_SNDBUF);
        this.sock.connect(server);
        this.sock.setSoTimeout(1);
    }

    // ====== DOWNLOAD ======
    public void download(String remoteName, File localFile, int chunk) throws Exception {
        if (chunk <= 0 || chunk > MAX_PAYLOAD) throw new IllegalArgumentException("chunk must be <= " + MAX_PAYLOAD);

        // send GET
        String meta = remoteName + "\n" + chunk + "\n";
        sendPacket(T_REQ_GET, 0, 0, 0, chunk, meta.getBytes(StandardCharsets.UTF_8));

        // wait INFO
        Packet info = waitInfo();
        if (info.type == T_ERR) throw new IOException(info.payloadText());

        String[] parts = info.payloadText().split("\\s+");
        if (parts.length < 3 || !"OK".equals(parts[0])) throw new IOException("Bad INFO: " + info.payloadText());
        int tid = info.transferId;
        long totalBytes = Long.parseLong(parts[2]);
        int totalChunks = info.totalChunks;
        int usedChunk = info.chunk;

        System.out.println("DOWNLOAD: tid=" + tid + " bytes=" + totalBytes + " chunk=" + usedChunk + " chunks=" + totalChunks);

        BitSet received = new BitSet(totalChunks);
        long lastRx = System.currentTimeMillis();

        try (RandomAccessFile raf = new RandomAccessFile(localFile, "rw")) {
            raf.setLength(totalBytes);

            // Phase 1: BLAST receive until DONE + idle
            while (true) {
                Packet p = recvPacket();
                long now = System.currentTimeMillis();

                if (p != null) {
                    if (p.transferId != tid) continue;

                    if (p.type == T_DATA) {
                        lastRx = now;
                        int seq = p.seq;
                        if (seq >= 0 && seq < totalChunks && !received.get(seq)) {
                            long off = (long) seq * usedChunk;
                            raf.seek(off);
                            raf.write(p.payload);
                            received.set(seq);
                        }
                    } else if (p.type == T_DONE) {
                        lastRx = now;
                        // ждём небольшой idle, чтобы не проспать последние данные
                    } else if (p.type == T_ERR) {
                        throw new IOException(p.payloadText());
                    }
                }

                if (received.nextClearBit(0) >= totalChunks) break;
                if (now - lastRx > IDLE_FINISH_MS) break;
            }

            // Phase 2: REPAIR rounds (NACK ranges)
            for (int round = 1; round <= REPAIR_ROUNDS; round++) {
                int missing = countMissing(received, totalChunks);
                if (missing == 0) break;

                byte[] nackPayload = buildNackRanges(received, totalChunks, MAX_NACK_RANGES);
                sendPacket(T_NACK, tid, 0, totalChunks, usedChunk, nackPayload);

                long repairUntil = System.currentTimeMillis() + 200; // короткий ремонт
                while (System.currentTimeMillis() < repairUntil) {
                    Packet p = recvPacket();
                    if (p == null) continue;
                    if (p.transferId != tid) continue;

                    if (p.type == T_DATA) {
                        int seq = p.seq;
                        if (seq >= 0 && seq < totalChunks && !received.get(seq)) {
                            long off = (long) seq * usedChunk;
                            raf.seek(off);
                            raf.write(p.payload);
                            received.set(seq);
                        }
                    } else if (p.type == T_ERR) {
                        throw new IOException(p.payloadText());
                    }
                }

                int missingAfter = countMissing(received, totalChunks);
                System.out.println("Repair round " + round + ": missing " + missing + " -> " + missingAfter);
            }
        }

        // tell server stop (DONE as client->server in our simplified scheme)
        sendPacket(T_DONE, tid, 0, 0, chunk, "OK".getBytes(StandardCharsets.UTF_8));

        int finalMissing = countMissing(received, (int)((new File(localFile.getPath()).length() + chunk - 1L)/chunk));
        if (finalMissing == 0) System.out.println("Saved OK (no missing chunks)");
        else System.out.println("Saved with some loss: missingChunks=" + finalMissing);
    }

    // ====== UPLOAD ======
    public void upload(File localFile, String remoteName, int chunk) throws Exception {
        if (!localFile.exists() || !localFile.isFile()) throw new FileNotFoundException(localFile.getPath());
        if (chunk <= 0 || chunk > MAX_PAYLOAD) throw new IllegalArgumentException("chunk must be <= " + MAX_PAYLOAD);

        long totalBytes = localFile.length();
        String meta = remoteName + "\n" + totalBytes + "\n" + chunk + "\n";
        sendPacket(T_REQ_PUT, 0, 0, 0, chunk, meta.getBytes(StandardCharsets.UTF_8));

        Packet info = waitInfo();
        if (info.type == T_ERR) throw new IOException(info.payloadText());

        int tid = info.transferId;
        int totalChunks = info.totalChunks;
        int usedChunk = info.chunk;

        System.out.println("UPLOAD: tid=" + tid + " bytes=" + totalBytes + " chunk=" + usedChunk + " chunks=" + totalChunks);

        try (RandomAccessFile raf = new RandomAccessFile(localFile, "r")) {
            // blast send without ACKs
            for (int seq = 0; seq < totalChunks; seq++) {
                long off = (long) seq * usedChunk;
                int len = (int) Math.min(usedChunk, totalBytes - off);
                byte[] data = new byte[len];
                raf.seek(off);
                raf.readFully(data);

                sendPacket(T_DATA, tid, seq, totalChunks, usedChunk, data);
            }
        }

        // tell server done
        sendPacket(T_DONE, tid, 0, totalChunks, usedChunk, "DONE".getBytes(StandardCharsets.UTF_8));
        System.out.println("UPLOAD finished (no per-packet ACKs)");
    }

    // ====== helpers ======
    private Packet waitInfo() throws IOException {
        long deadline = System.currentTimeMillis() + 3000;
        while (System.currentTimeMillis() < deadline) {
            Packet p = recvPacket();
            if (p == null) continue;
            if (p.type == T_INFO || p.type == T_ERR) return p;
        }
        throw new SocketTimeoutException("INFO timeout");
    }

    private Packet recvPacket() throws IOException {
        byte[] buf = new byte[65507];
        DatagramPacket dp = new DatagramPacket(buf, buf.length);
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

    private void sendPacket(byte type, int tid, int seq, int totalChunks, int chunk, byte[] payload) throws IOException {
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

        DatagramPacket dp = new DatagramPacket(bb.array(), bb.position(), server);
        sock.send(dp);
    }

    private static int countMissing(BitSet received, int totalChunks) {
        int miss = 0;
        for (int i = received.nextClearBit(0); i >= 0 && i < totalChunks; i = received.nextClearBit(i + 1)) {
            miss++;
        }
        return miss;
    }

    // строим ranges: start + len (int,int) * N
    private static byte[] buildNackRanges(BitSet received, int totalChunks, int maxRanges) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ByteBuffer bb = ByteBuffer.allocate(maxRanges * 8);

        int ranges = 0;
        int i = received.nextClearBit(0);
        while (i >= 0 && i < totalChunks && ranges < maxRanges) {
            int start = i;
            int end = start;
            while (end < totalChunks && !received.get(end)) end++;
            int len = end - start;

            bb.putInt(start);
            bb.putInt(len);
            ranges++;

            i = received.nextClearBit(end);
        }
        return Arrays.copyOf(bb.array(), bb.position());
    }

    private static void usage() {
        System.out.println("""
            Usage:
              java org.example.UdpFastFileClient <host> <port> download <remoteName> <localPath> [chunk=1472]
              java org.example.UdpFastFileClient <host> <port> upload   <localPath> <remoteName> [chunk=1472]
            """);
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 4) { usage(); return; }
        String host = args[0];
        int port = Integer.parseInt(args[1]);
        String cmd = args[2].toLowerCase(Locale.ROOT);

        int chunk = (args.length >= 6) ? Integer.parseInt(args[5]) : MAX_PAYLOAD;
        InetSocketAddress server = new InetSocketAddress(host, port);
        UdpFastFileClient c = new UdpFastFileClient(server);

        if ("download".equals(cmd)) {
            String remote = args[3];
            File local = new File(args[4]);
            c.download(remote, local, chunk);
        } else if ("upload".equals(cmd)) {
            File local = new File(args[3]);
            String remote = args[4];
            c.upload(local, remote, chunk);
        } else {
            usage();
        }
    }
}
