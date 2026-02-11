package org.example;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;

public final class NioTcpFileServer {

    private static final int DEFAULT_PORT = 50505;
    private static final String DEFAULT_DIR = "server_storage";

    // === "порция" обслуживания (чтобы не блокировать остальных) ===
    private static final int CHUNK = 32 * 1024;          // 8KB per step
    private static final int READ_BUF = 64 * 1024;      // per client
    private static final int SELECT_TIMEOUT_MS = 10;    // small for interactivity

    private final int port;
    private final File baseDir;

    public NioTcpFileServer(int port, File baseDir) {
        this.port = port;
        this.baseDir = baseDir;
    }

    public void start() throws IOException {
        ensureDir(baseDir);

        try (Selector selector = Selector.open();
             ServerSocketChannel server = ServerSocketChannel.open()) {

            server.configureBlocking(false);
            server.bind(new InetSocketAddress(port));
            server.register(selector, SelectionKey.OP_ACCEPT);

            System.out.println("NIO TCP server started (single-thread, multiplexing)");
            System.out.println("Port: " + port);
            System.out.println("Storage: " + baseDir.getAbsolutePath());
            System.out.println("Waiting for clients...\n");

            while (true) {
                selector.select(SELECT_TIMEOUT_MS);

                Iterator<SelectionKey> it = selector.selectedKeys().iterator();
                while (it.hasNext()) {
                    SelectionKey key = it.next();
                    it.remove();

                    try {
                        if (!key.isValid()) continue;

                        if (key.isAcceptable()) {
                            acceptLoop(server, selector);
                        } else {
                            Client c = (Client) key.attachment();
                            if (c == null) continue;

                            if (key.isReadable()) {
                                onReadable(key, c);
                            }
                            if (key.isWritable()) {
                                onWritable(key, c);
                            }

                            // обновляем интересы (READ всегда, WRITE только если есть что писать)
                            int ops = SelectionKey.OP_READ;
                            if (c.hasPendingWrite()) ops |= SelectionKey.OP_WRITE;
                            key.interestOps(ops);
                        }
                    } catch (CancelledKeyException ignored) {
                        // client closed
                    } catch (IOException e) {
                        closeKey(key, "IO error: " + e.getMessage());
                    } catch (Exception e) {
                        closeKey(key, "Session error: " + e.getMessage());
                    }
                }
            }
        }
    }

    private void acceptLoop(ServerSocketChannel server, Selector selector) throws IOException {
        while (true) {
            SocketChannel ch = server.accept();
            if (ch == null) break;

            ch.configureBlocking(false);
            ch.setOption(java.net.StandardSocketOptions.TCP_NODELAY, true);
            ch.setOption(java.net.StandardSocketOptions.SO_KEEPALIVE, true);

            SelectionKey key = ch.register(selector, SelectionKey.OP_READ);
            Client c = new Client(ch);
            key.attach(c);

            System.out.println("Client connected: " + ch.getRemoteAddress());
        }
    }

    private void onReadable(SelectionKey key, Client c) throws IOException {
        SocketChannel ch = (SocketChannel) key.channel();

        // readBuf уже должен быть в WRITE-mode (после прошлого compact()).
        int n = ch.read(c.readBuf);
        if (n == -1) { closeKey(key, "EOF"); return; }
        if (n == 0) return;

        c.readBuf.flip(); // теперь READ-mode

        int budget = CHUNK;

        while (budget > 0 && c.readBuf.hasRemaining()) {
            if (c.mode == Mode.CMD) {
                byte b = c.readBuf.get();
                budget--;

                if (b == '\n') {
                    String line = c.cmdLine.toString(StandardCharsets.UTF_8).trim();
                    c.cmdLine.reset();
                    if (!line.isEmpty()) handleCommand(line, c);
                } else if (b != '\r') {
                    c.cmdLine.write(b);
                    if (c.cmdLine.size() > 8 * 1024) {
                        c.enqueueLine("ERR Line too long");
                        c.cmdLine.reset();
                    }
                }
            } else if (c.mode == Mode.RECV_FILE) {
                int can = (int) Math.min((long) budget, c.recvRemaining);
                if (can <= 0) break;

                int take = Math.min(c.readBuf.remaining(), can);

                int oldLimit = c.readBuf.limit();
                c.readBuf.limit(c.readBuf.position() + take);

                int wrote = c.recvFile.write(c.readBuf);

                c.readBuf.limit(oldLimit);

                if (wrote > 0) {
                    c.recvRemaining -= wrote;
                    budget -= wrote;
                } else break;

                if (c.recvRemaining == 0) {
                    closeQuiet(c.recvFile);
                    c.recvFile = null;
                    c.mode = Mode.CMD;
                    c.enqueueLine("OK DONE " + c.recvTotalSize);
                }
            } else {
                break;
            }
        }

        c.readBuf.compact(); // обратно в WRITE-mode, хвост сохранён
    }

    // === WRITE handler (one chunk per select tick) ===
    private void onWritable(SelectionKey key, Client c) throws IOException {
        SocketChannel ch = (SocketChannel) key.channel();

        // 1) сначала выталкиваем очередь текстовых/буферных ответов (например "OK <total>")
        int budget = CHUNK;
        while (budget > 0 && !c.outQueue.isEmpty()) {
            ByteBuffer buf = c.outQueue.peekFirst();
            int before = buf.remaining();
            ch.write(buf);
            int after = buf.remaining();

            int sent = before - after;
            budget -= sent;

            if (!buf.hasRemaining()) c.outQueue.pollFirst();
            if (sent == 0) break; // socket backpressure
        }

        // 2) если режим SEND_FILE — отправляем кусок файла (но только если header уже ушёл)
        if (budget > 0 && c.mode == Mode.SEND_FILE && c.outQueue.isEmpty()) {
            sendFileChunk(ch, c, budget);
        }

        if (c.closeAfterWrite && c.outQueue.isEmpty() && c.mode == Mode.CMD) {
            closeKey(key, "BYE");
        }
    }

    private void sendFileChunk(SocketChannel ch, Client c, int budget) throws IOException {
        if (c.sendRemaining <= 0) { finishSend(c); return; }

        if (c.fileOutBuf == null) {
            c.fileOutBuf = ByteBuffer.allocate(CHUNK);
            c.fileOutBuf.limit(0); // ✅ делаем буфер "пустым", чтобы hasRemaining() стало false
        }

        // Если буфер пустой — подгружаем следующую порцию из файла
        if (c.fileOutBuf.remaining() == 0) {
            c.fileOutBuf.clear();
            int toRead = (int) Math.min(c.fileOutBuf.capacity(), Math.min((long) budget, c.sendRemaining));
            c.fileOutBuf.limit(toRead);

            int r = c.sendFile.read(c.fileOutBuf);
            if (r == -1) {
                closeQuiet(c.sendFile);
                c.sendFile = null;
                c.mode = Mode.CMD;
                c.enqueueLine("ERR File read error");
                c.fileOutBuf.clear();
                return;
            }
            c.fileOutBuf.flip();
        }

        if (!c.fileOutBuf.hasRemaining()) return;

        int before = c.fileOutBuf.remaining();
        ch.write(c.fileOutBuf);
        int sent = before - c.fileOutBuf.remaining();

        c.sendRemaining -= sent;

        if (c.sendRemaining == 0 && !c.fileOutBuf.hasRemaining()) {
            finishSend(c);
        }
    }

    private void finishSend(Client c) {
        closeQuiet(c.sendFile);
        c.sendFile = null;
        c.fileOutBuf = null;
        c.mode = Mode.CMD;
        c.enqueueLine("OK DONE " + c.sendTotalSize);
    }

    // === COMMANDS ===
    private void handleCommand(String line, Client c) throws IOException {
        String[] parts = line.split("\\s+");
        String cmd = parts[0].toUpperCase();

        switch (cmd) {
            case "ECHO" -> {
                String payload = line.length() > 4 ? line.substring(4).trim() : "";
                c.enqueueLine("OK " + payload);
            }
            case "TIME" -> c.enqueueLine("OK " + Instant.now());
            case "CLOSE", "EXIT", "QUIT" -> {
                c.enqueueLine("BYE");
                c.closeAfterWrite = true;
            }
            case "UPLOAD" -> handleUpload(parts, c);
            case "DOWNLOAD" -> handleDownload(parts, c);
            default -> c.enqueueLine("ERR Unknown command");
        }
    }

    // UPLOAD <name> <totalSize> <offset>
    private void handleUpload(String[] parts, Client c) throws IOException {
        if (parts.length < 4) {
            c.enqueueLine("ERR Usage: UPLOAD <name> <size> <offset>");
            return;
        }
        String name = sanitizeFileName(parts[1]);
        long totalSize = parseLong(parts[2], -1);
        long offset = parseLong(parts[3], -1);

        if (name == null || totalSize < 0 || offset < 0 || offset > totalSize) {
            c.enqueueLine("ERR Bad args");
            return;
        }

        File target = new File(baseDir, name);
        ensureDir(target.getParentFile());

        long current = target.exists() ? target.length() : 0L;
        if (offset != current) {
            c.enqueueLine("ERR Offset mismatch. Server has " + current);
            return;
        }

        // готовимся принимать бинарь
        c.recvFile = FileChannel.open(target.toPath(),
                java.nio.file.StandardOpenOption.CREATE,
                java.nio.file.StandardOpenOption.WRITE);
        c.recvFile.position(offset);
        c.recvTotalSize = totalSize;
        c.recvRemaining = totalSize - offset;
        c.mode = Mode.RECV_FILE;

        c.enqueueLine("OK");
        if (c.recvRemaining == 0) {
            // пустой догруз
            closeQuiet(c.recvFile);
            c.recvFile = null;
            c.mode = Mode.CMD;
            c.enqueueLine("OK DONE " + totalSize);
        }
    }

    // DOWNLOAD <name> <offset>
    private void handleDownload(String[] parts, Client c) throws IOException {
        if (parts.length < 3) {
            c.enqueueLine("ERR Usage: DOWNLOAD <name> <offset>");
            return;
        }
        String name = sanitizeFileName(parts[1]);
        long offset = parseLong(parts[2], -1);
        if (name == null || offset < 0) {
            c.enqueueLine("ERR Bad args");
            return;
        }

        File source = new File(baseDir, name);
        if (!source.exists() || !source.isFile()) {
            c.enqueueLine("ERR No such file");
            return;
        }

        long total = source.length();
        if (offset > total) {
            c.enqueueLine("ERR Offset too big");
            return;
        }

        c.sendFile = FileChannel.open(source.toPath(), java.nio.file.StandardOpenOption.READ);
        c.sendFile.position(offset);
        c.sendTotalSize = total;
        c.sendRemaining = total - offset;
        c.mode = Mode.SEND_FILE;

        // сначала шлём заголовок, потом байты, потом DONE
        c.enqueueLine("OK " + total);
        if (c.sendRemaining == 0) {
            finishSend(c);
        }
    }

    // === Utils / cleanup ===
    private void closeKey(SelectionKey key, String reason) {
        try {
            Client c = (Client) key.attachment();
            if (c != null) c.closeAll();
            key.channel().close();
        } catch (Exception ignored) {}
        try { key.cancel(); } catch (Exception ignored) {}
        // reason можно логировать
        // System.out.println("Client closed: " + reason);
    }

    private static void closeQuiet(Channel ch) {
        try { if (ch != null) ch.close(); } catch (Exception ignored) {}
    }

    private void ensureDir(File dir) throws IOException {
        if (dir == null) return;
        if (!dir.exists() && !dir.mkdirs()) {
            throw new IOException("Cannot create dir: " + dir);
        }
    }

    private String sanitizeFileName(String raw) {
        if (raw == null || raw.isBlank()) return null;
        raw = raw.replace("\\", "/");
        if (raw.contains("..") || raw.startsWith("/") || raw.contains("/")) return null;
        return raw;
    }

    private long parseLong(String s, long def) {
        try { return Long.parseLong(s); } catch (Exception e) { return def; }
    }

    private enum Mode { CMD, RECV_FILE, SEND_FILE }

    private static final class Client {
        final SocketChannel ch;

        final ByteBuffer readBuf = ByteBuffer.allocate(READ_BUF);
        final java.io.ByteArrayOutputStream cmdLine = new java.io.ByteArrayOutputStream(256);

        final Deque<ByteBuffer> outQueue = new ArrayDeque<>();
        boolean closeAfterWrite = false;

        Mode mode = Mode.CMD;

        // recv file
        FileChannel recvFile;
        long recvRemaining;
        long recvTotalSize;

        // send file
        FileChannel sendFile;
        long sendRemaining;
        long sendTotalSize;
        ByteBuffer fileOutBuf; // chunk buffer for file bytes

        Client(SocketChannel ch) {
            this.ch = ch;
        }

        void enqueueLine(String line) {
            byte[] data = (line + "\n").getBytes(StandardCharsets.UTF_8);
            outQueue.addLast(ByteBuffer.wrap(data));
        }

        boolean hasPendingWrite() {
            if (!outQueue.isEmpty()) return true;
            if (mode == Mode.SEND_FILE && sendRemaining > 0) return true;
            if (closeAfterWrite && outQueue.isEmpty() && mode == Mode.CMD) return true;
            return false;
        }

        void closeAll() {
            closeQuiet(recvFile);
            closeQuiet(sendFile);
            recvFile = null;
            sendFile = null;
            try { ch.close(); } catch (Exception ignored) {}
        }
    }

    public static void main(String[] args) throws Exception {
        int port = args.length > 0 ? Integer.parseInt(args[0]) : DEFAULT_PORT;
        File dir = new File(args.length > 1 ? args[1] : DEFAULT_DIR);
        new NioTcpFileServer(port, dir).start();
    }
}
