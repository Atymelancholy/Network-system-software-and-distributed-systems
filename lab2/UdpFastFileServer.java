package org.example;

import java.io.*;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;

public final class UdpFastFileServer {

    private static final int PORT_DEFAULT = 50506;
    private static final String DIR_DEFAULT = "server_storage_udp_fast";
    private static final int MTU = 1500;
    private static final int IP_HDR = 20;
    private static final int UDP_HDR = 8;
    private static final int UDP_PAYLOAD_MAX = MTU - IP_HDR - UDP_HDR;
    private static final int HDR = 20;
    private static final int MAX_PAYLOAD = UDP_PAYLOAD_MAX - HDR;
    private static final int WINDOW = 512;
    private static final int RTO_MS = 80;
    private static final int MAX_RETRIES = 25;
    private static final int ACK_EVERY_PKTS = 64;
    private static final int ACK_IDLE_MS = 15;
    private static final int DEAD_MS = 5000;
    private static final int MAX_ACK_PACKET = 1200;
    private static final int ACK_OVERHEAD = HDR + 4;
    private static final int MAX_ACK_RANGES = Math.max(0, (MAX_ACK_PACKET - ACK_OVERHEAD) / 8);
    private static final int SO_RCVBUF = 8 * 1024 * 1024;
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

    private static final class Transfer {
        final boolean upload;
        final SocketAddress peer;
        final int transferId;
        final File file;
        final RandomAccessFile raf;
        final long totalBytes;
        final int chunk;
        final int totalChunks;

        final BitSet received;
        int rxSinceAck = 0;
        long lastAckSentMs = 0;

        final BitSet acked;
        int base = 0;
        int nextToSend = 0;
        final long[] lastSendMs;
        final byte[] retries;
        boolean doneSent = false;

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
            this.acked = new BitSet(totalChunks);

            this.lastSendMs = new long[totalChunks];
            this.retries = new byte[totalChunks];
        }

        void touchRx() { lastRxMs = System.currentTimeMillis(); }

        void closeQuiet() { try { raf.close(); } catch (Exception ignored) {} }

        boolean allReceived() {
            return received.nextClearBit(0) >= totalChunks;
        }

        boolean allAcked() {
            return acked.nextClearBit(0) >= totalChunks;
        }
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

        this.sock.setSoTimeout(2);
    }

    public void run() throws Exception {
        System.out.println("UDP FAST+ACK server on port " + sock.getLocalPort());
        System.out.println("Storage: " + baseDir.getAbsolutePath());
        System.out.println("MAX_PAYLOAD=" + MAX_PAYLOAD + " WINDOW=" + WINDOW + " RTO_MS=" + RTO_MS);

        byte[] buf = new byte[65507];
        DatagramPacket dp = new DatagramPacket(buf, buf.length);

        while (true) {
            if (current != null) {
                serviceTransfer(current);
            }

            Packet p = recvPacket(dp);
            if (p == null) continue;

            SocketAddress peer = dp.getSocketAddress();

            switch (p.type) {
                case T_REQ_GET -> handleGet(peer, p);
                case T_REQ_PUT -> handlePut(peer, p);
                case T_DATA    -> handleData(peer, p);
                case T_ACK     -> handleAck(peer, p);
                case T_DONE    -> handleDone(peer, p);
                default -> {}
            }
        }
    }

    private void serviceTransfer(Transfer t) throws Exception {
        long now = System.currentTimeMillis();

        if (now - t.lastRxMs > DEAD_MS) {
            System.out.println("Transfer timeout / connection lost, tid=" + t.transferId);
            t.closeQuiet();
            current = null;
            return;
        }

        if (!t.upload) {
            sendNewWithinWindow(t, now);
            resendTimeouts(t, now);

            if (t.allAcked() && !t.doneSent) {
                sendPacket(t.peer, T_DONE, t.transferId, 0, t.totalChunks, t.chunk,
                        "DONE".getBytes(StandardCharsets.UTF_8));
                t.doneSent = true;
            }
        } else {
            maybeSendAck(t, now);

            if (t.allReceived() && !t.doneSent) {
                sendPacket(t.peer, T_DONE, t.transferId, 0, t.totalChunks, t.chunk,
                        "DONE".getBytes(StandardCharsets.UTF_8));
                t.doneSent = true;
            }
        }
    }

    private void handleGet(SocketAddress peer, Packet req) throws Exception {
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

        if (current != null) {
            sendErr(peer, 0, "Server busy (single transfer)");
            return;
        }

        long total = f.length();
        int totalChunks = (int) ((total + chunk - 1L) / chunk);
        int tid = rnd.nextInt();

        RandomAccessFile raf = new RandomAccessFile(f, "r");
        Transfer t = new Transfer(false, peer, tid, f, raf, total, chunk, totalChunks);
        current = t;

        String info = "OK " + name + " " + total + "\n";
        sendPacket(peer, T_INFO, tid, 0, totalChunks, chunk, info.getBytes(StandardCharsets.UTF_8));

        t.base = 0;
        t.nextToSend = 0;
        t.doneSent = false;
        t.touchRx();
    }

    private void handlePut(SocketAddress peer, Packet req) throws Exception {
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

        if (current != null) {
            sendErr(peer, 0, "Server busy (single transfer)");
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

        t.doneSent = false;
        t.lastAckSentMs = 0;
        t.rxSinceAck = 0;
        t.touchRx();
    }

    private void handleData(SocketAddress peer, Packet data) throws Exception {
        Transfer t = current;
        if (t == null) return;
        if (!t.upload) return;
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
            t.rxSinceAck++;
        }

        long now = System.currentTimeMillis();
        if (t.rxSinceAck >= ACK_EVERY_PKTS) {
            sendAckPacket(t, now);
        }
    }

    private void handleAck(SocketAddress peer, Packet ack) throws Exception {
        Transfer t = current;
        if (t == null) return;
        if (t.upload) return;
        if (!Objects.equals(t.peer, peer)) return;
        if (t.transferId != ack.transferId) return;

        t.touchRx();

        ByteBuffer bb = ByteBuffer.wrap(ack.payload);
        if (bb.remaining() < 4) return;

        int base = bb.getInt();
        if (base < 0) base = 0;
        if (base > t.totalChunks) base = t.totalChunks;

        if (base > 0) t.acked.set(0, base);

        int ranges = Math.min(bb.remaining() / 8, MAX_ACK_RANGES);
        for (int i = 0; i < ranges; i++) {
            int start = bb.getInt();
            int len = bb.getInt();
            if (len <= 0) continue;
            int end = Math.min(t.totalChunks, start + len);
            if (start < 0) start = 0;
            if (start < end) t.acked.set(start, end);
        }

        int newBase = t.acked.nextClearBit(t.base);
        if (newBase < 0) newBase = t.totalChunks;
        t.base = Math.min(newBase, t.totalChunks);

        if (t.nextToSend < t.base) t.nextToSend = t.base;
    }

    private void handleDone(SocketAddress peer, Packet done) throws Exception {
        Transfer t = current;
        if (t == null) return;
        if (!Objects.equals(t.peer, peer)) return;
        if (t.transferId != done.transferId) return;
        t.closeQuiet();
        current = null;
    }

    private void sendNewWithinWindow(Transfer t, long now) throws Exception {
        int limit = Math.min(t.totalChunks, t.base + WINDOW);

        while (t.nextToSend < limit) {
            int seq = t.nextToSend++;

            if (t.acked.get(seq)) continue;

            sendDataChunk(t, seq, now);
        }
    }

    private void resendTimeouts(Transfer t, long now) throws Exception {
        int limit = Math.min(t.totalChunks, t.base + WINDOW);

        for (int seq = t.base; seq < limit; seq++) {
            if (t.acked.get(seq)) continue;

            long last = t.lastSendMs[seq];
            if (last == 0) continue;

            if (now - last >= RTO_MS) {
                int r = Byte.toUnsignedInt(t.retries[seq]);
                if (r >= MAX_RETRIES) {
                    System.out.println("Max retries reached, tid=" + t.transferId + " seq=" + seq);
                    sendErr(t.peer, t.transferId, "Timeout / peer lost");
                    t.closeQuiet();
                    current = null;
                    return;
                }
                t.retries[seq] = (byte) (r + 1);
                sendDataChunk(t, seq, now);
            }
        }
    }

    private void sendDataChunk(Transfer t, int seq, long now) throws Exception {
        long off = (long) seq * t.chunk;
        int len = (int) Math.min(t.chunk, t.totalBytes - off);

        byte[] data = new byte[len];
        t.raf.seek(off);
        t.raf.readFully(data);

        sendPacket(t.peer, T_DATA, t.transferId, seq, t.totalChunks, t.chunk, data);
        t.lastSendMs[seq] = now;
    }

    private void maybeSendAck(Transfer t, long now) throws IOException {
        if (!t.upload) return;

        if (t.rxSinceAck > 0 && (now - t.lastAckSentMs) >= ACK_IDLE_MS) {
            sendAckPacket(t, now);
        }
    }

    private void sendAckPacket(Transfer t, long now) throws IOException {

        int base = t.received.nextClearBit(0);
        if (base < 0) base = t.totalChunks;
        if (base > t.totalChunks) base = t.totalChunks;

        byte[] sackRanges = buildReceivedSackRanges(t.received, base, t.totalChunks, MAX_ACK_RANGES);

        ByteBuffer bb = ByteBuffer.allocate(4 + sackRanges.length);
        bb.putInt(base);
        bb.put(sackRanges);

        sendPacket(t.peer, T_ACK, t.transferId, 0, t.totalChunks, t.chunk, Arrays.copyOf(bb.array(), bb.position()));

        t.lastAckSentMs = now;
        t.rxSinceAck = 0;
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
