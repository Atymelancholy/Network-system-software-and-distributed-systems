package org.example;

import java.io.*;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;

public final class UdpFastFileClient {

    private static final int MTU = 1500;
    private static final int IP_HDR = 20;
    private static final int UDP_HDR = 8;
    private static final int UDP_PAYLOAD_MAX = MTU - IP_HDR - UDP_HDR;
    private static final int HDR = 20;
    private static final int MAX_PAYLOAD = UDP_PAYLOAD_MAX - HDR;
    private static final int WINDOW = 512;
    private static final int RTO_MS = 80;
    private static final int MAX_RETRIES = 25;
    private static final int DEAD_MS = 5000;
    private static final int ACK_EVERY_PKTS = 64;
    private static final int ACK_IDLE_MS = 15;
    private static final int MAX_ACK_PACKET = 1200;
    private static final int ACK_OVERHEAD = HDR + 4;
    private static final int MAX_ACK_RANGES = Math.max(0, (MAX_ACK_PACKET - ACK_OVERHEAD) / 8);
    private static final int SO_RCVBUF = 16 * 1024 * 1024;
    private static final int SO_SNDBUF = 8 * 1024 * 1024;
    private static final short MAGIC = (short) 0x5446;
    private static final byte VER = 2;
    private static final byte T_REQ_GET  = 1;
    private static final byte T_REQ_PUT  = 2;
    private static final byte T_INFO     = 3;
    private static final byte T_DATA     = 4;
    private static final byte T_DONE     = 6;
    private static final byte T_ERR      = 7;
    private static final byte T_ACK      = 8;

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
    private final byte[] rbuf = new byte[65507];
    private final DatagramPacket rdp = new DatagramPacket(rbuf, rbuf.length);

    public UdpFastFileClient(InetSocketAddress server) throws Exception {
        this.server = server;
        this.sock = new DatagramSocket();
        this.sock.setReceiveBufferSize(SO_RCVBUF);
        this.sock.setSendBufferSize(SO_SNDBUF);

        this.sock.connect(server);

        this.sock.setSoTimeout(2);
    }

    private static void printSpeed(String label, long bytes, long t0Ns, long t1Ns) {
        double sec = (t1Ns - t0Ns) / 1_000_000_000.0;
        if (sec <= 0) sec = 1e-9;
        double mbit = (bytes * 8.0) / (1024.0*1024.0);
        double mbps = mbit / sec;
        System.out.printf("%s speed: %.2f Mbit/s (%d bytes in %.3f s)%n", label, mbps, bytes, sec);
    }

    public void download(String remoteName, File localFile, int chunk) throws Exception {
        if (chunk <= 0 || chunk > MAX_PAYLOAD) throw new IllegalArgumentException("chunk must be <= " + MAX_PAYLOAD);

        String meta = remoteName + "\n" + chunk + "\n";
        sendPacket(T_REQ_GET, 0, 0, 0, chunk, meta.getBytes(StandardCharsets.UTF_8));

        Packet info = waitInfo();
        if (info.type == T_ERR) throw new IOException(info.payloadText());

        String[] parts = info.payloadText().split("\\s+");
        if (parts.length < 3 || !"OK".equals(parts[0])) throw new IOException("Bad INFO: " + info.payloadText());

        int tid = info.transferId;
        long totalBytes = Long.parseLong(parts[2]);
        int totalChunks = info.totalChunks;
        int usedChunk = info.chunk;

        System.out.println("DOWNLOAD: tid=" + tid + " bytes=" + totalBytes + " chunk=" + usedChunk + " chunks=" + totalChunks);

        long t0 = System.nanoTime();
        long firstDataRxNs = 0;
        long allReceivedNs = 0;

        BitSet received = new BitSet(totalChunks);
        long lastRxMs = System.currentTimeMillis();

        int rxSinceAck = 0;
        long lastAckSentMs = 0;
        boolean doneFromServer = false;

        try (RandomAccessFile raf = new RandomAccessFile(localFile, "rw")) {
            raf.setLength(totalBytes);

            while (true) {
                long now = System.currentTimeMillis();

                if (now - lastRxMs > DEAD_MS) {
                    throw new SocketTimeoutException("DOWNLOAD timeout / connection lost");
                }

                Packet p = recvPacket();
                if (p != null) {
                    if (p.transferId != tid) continue;

                    lastRxMs = now;

                    if (p.type == T_DATA) {
                        int seq = p.seq;
                        if (seq >= 0 && seq < totalChunks && !received.get(seq)) {
                            long off = (long) seq * usedChunk;
                            int expected = (int) Math.min(usedChunk, totalBytes - off);
                            if (p.payload.length != expected) continue;

                            raf.seek(off);
                            raf.write(p.payload);
                            received.set(seq);
                            rxSinceAck++;

                            if (firstDataRxNs == 0) firstDataRxNs = System.nanoTime();
                        }
                    } else if (p.type == T_ERR) {
                        throw new IOException(p.payloadText());
                    }
                }

                if (rxSinceAck >= ACK_EVERY_PKTS || (rxSinceAck > 0 && (now - lastAckSentMs) >= ACK_IDLE_MS)) {
                    byte[] ackPayload = buildAckPayload(received, totalChunks, MAX_ACK_RANGES);
                    sendPacket(T_ACK, tid, 0, totalChunks, usedChunk, ackPayload);
                    lastAckSentMs = now;
                    rxSinceAck = 0;
                }

                if (received.nextClearBit(0) >= totalChunks) {
                    allReceivedNs = System.nanoTime();
                    byte[] ackPayload = buildAckPayload(received, totalChunks, MAX_ACK_RANGES);
                    sendPacket(T_ACK, tid, 0, totalChunks, usedChunk, ackPayload);
                    break;
                }
            }
        }

        sendPacket(T_DONE, tid, 0, 0, chunk, "OK".getBytes(StandardCharsets.UTF_8));

        long t1 = System.nanoTime();
        long startNs = (firstDataRxNs != 0) ? firstDataRxNs : t0;
        long endNs   = (allReceivedNs != 0) ? allReceivedNs : System.nanoTime();
        printSpeed("DOWNLOAD_REAL", totalBytes, startNs, endNs);

        int missing = countMissing(received, totalChunks);
        if (missing == 0) System.out.println("Saved OK (no missing chunks)");
        else System.out.println("Saved with loss: missingChunks=" + missing);
    }

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

        long t0 = System.nanoTime();
        long firstDataSentNs = 0;
        long doneRxNs = 0;
        boolean gotDone = false;

        BitSet acked = new BitSet(totalChunks);
        int base = 0;
        int nextToSend = 0;

        long[] lastSendMs = new long[totalChunks];
        byte[] retries = new byte[totalChunks];

        long lastRxMs = System.currentTimeMillis();

        try (RandomAccessFile raf = new RandomAccessFile(localFile, "r")) {
            while (true) {
                long now = System.currentTimeMillis();

                if (now - lastRxMs > DEAD_MS) {
                    throw new SocketTimeoutException("UPLOAD timeout / connection lost");
                }

                int limit = Math.min(totalChunks, base + WINDOW);
                while (nextToSend < limit) {
                    int seq = nextToSend++;
                    if (acked.get(seq)) continue;

                    sendDataFromFile(raf, tid, seq, totalChunks, usedChunk, totalBytes);
                    if (firstDataSentNs == 0) firstDataSentNs = System.nanoTime();
                    lastSendMs[seq] = now;
                }

                Packet p = recvPacket();
                if (p != null) {
                    if (p.transferId != tid) {
                    } else {
                        lastRxMs = now;

                        if (p.type == T_ACK) {
                            applyAckPayloadToBitset(p.payload, acked, totalChunks);
                            int newBase = acked.nextClearBit(base);
                            if (newBase < 0) newBase = totalChunks;
                            base = Math.min(newBase, totalChunks);

                            if (nextToSend < base) nextToSend = base;
                        } else if (p.type == T_DONE) {
                            gotDone = true;
                            doneRxNs = System.nanoTime();
                        } else if (p.type == T_ERR) {
                            throw new IOException(p.payloadText());
                        }
                    }
                }

                limit = Math.min(totalChunks, base + WINDOW);
                for (int seq = base; seq < limit; seq++) {
                    if (acked.get(seq)) continue;

                    long last = lastSendMs[seq];
                    if (last == 0) continue;

                    if (now - last >= RTO_MS) {
                        int r = Byte.toUnsignedInt(retries[seq]);
                        if (r >= MAX_RETRIES) {
                            throw new SocketTimeoutException("Max retries reached: seq=" + seq);
                        }
                        retries[seq] = (byte) (r + 1);
                        sendDataFromFile(raf, tid, seq, totalChunks, usedChunk, totalBytes);
                        lastSendMs[seq] = now;
                    }
                }
                if (acked.nextClearBit(0) >= totalChunks && gotDone) {
                    break;
                }
            }
        }

        sendPacket(T_DONE, tid, 0, totalChunks, usedChunk, "DONE".getBytes(StandardCharsets.UTF_8));

        long t1 = System.nanoTime();
        long startNs = (firstDataSentNs != 0) ? firstDataSentNs : t0;
        long endNs   = (doneRxNs != 0) ? doneRxNs : System.nanoTime();
        printSpeed("UPLOAD_REAL", totalBytes, startNs, endNs);

        System.out.println("UPLOAD finished (window+ACK+retransmit)");
    }

    private void sendDataFromFile(RandomAccessFile raf, int tid, int seq, int totalChunks, int usedChunk, long totalBytes) throws IOException {
        long off = (long) seq * usedChunk;
        int len = (int) Math.min(usedChunk, totalBytes - off);

        byte[] data = new byte[len];
        raf.seek(off);
        raf.readFully(data);

        sendPacket(T_DATA, tid, seq, totalChunks, usedChunk, data);
    }

    private static byte[] buildAckPayload(BitSet received, int totalChunks, int maxRanges) {
        int base = received.nextClearBit(0);
        if (base < 0) base = totalChunks;
        if (base > totalChunks) base = totalChunks;

        byte[] sackRanges = buildReceivedSackRanges(received, base, totalChunks, maxRanges);

        ByteBuffer bb = ByteBuffer.allocate(4 + sackRanges.length);
        bb.putInt(base);
        bb.put(sackRanges);
        return Arrays.copyOf(bb.array(), bb.position());
    }

    private static byte[] buildReceivedSackRanges(BitSet received, int from, int totalChunks, int maxRanges) {
        ByteBuffer bb = ByteBuffer.allocate(maxRanges * 8);
        int ranges = 0;

        int i = received.nextSetBit(from);
        while (i >= 0 && i < totalChunks && ranges < maxRanges) {
            int start = i;
            int end = i + 1;
            while (end < totalChunks && received.get(end)) end++;

            bb.putInt(start);
            bb.putInt(end - start);
            ranges++;

            i = received.nextSetBit(end);
        }
        return Arrays.copyOf(bb.array(), bb.position());
    }

    private static void applyAckPayloadToBitset(byte[] payload, BitSet acked, int totalChunks) {
        ByteBuffer bb = ByteBuffer.wrap(payload);
        if (bb.remaining() < 4) return;

        int base = bb.getInt();
        if (base < 0) base = 0;
        if (base > totalChunks) base = totalChunks;

        if (base > 0) acked.set(0, base);

        int ranges = bb.remaining() / 8;
        for (int i = 0; i < ranges; i++) {
            int start = bb.getInt();
            int len = bb.getInt();
            if (len <= 0) continue;

            if (start < 0) start = 0;
            int end = Math.min(totalChunks, start + len);
            if (start < end) acked.set(start, end);
        }
    }

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
        try {
            sock.receive(rdp);
        } catch (SocketTimeoutException e) {
            return null;
        } catch (PortUnreachableException e) {
            throw new SocketException("Server unreachable (ICMP Port Unreachable / REJECT)");
        }

        ByteBuffer bb = ByteBuffer.wrap(rdp.getData(), rdp.getOffset(), rdp.getLength());
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
