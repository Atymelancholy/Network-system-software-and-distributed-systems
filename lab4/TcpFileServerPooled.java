package org.example;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicBoolean;

public final class TcpFileServerPooled {

    private static final int DEFAULT_PORT = 50505;
    private static final String DEFAULT_DIR = "server_storage";

    // === ЛР4: пул потоков ===
    private static final int NMIN = 4;
    private static final int NMAX = 64;
    private static final long EXTRA_IDLE_TIMEOUT_MS = 10_000;

    // === МНОГО acceptor-ов ===
    private static final int ACCEPTORS = 4; // можно = NMIN или фиксированно 2..8

    private static final int IO_BUF = 64 * 1024;
    private static final int MAX_LINE = 8 * 1024;

    private final int port;
    private final File baseDir;

    // защита accept (взаимное исключение)
    private final Object acceptLock = new Object();

    public TcpFileServerPooled(int port, File baseDir) {
        this.port = port;
        this.baseDir = baseDir;
    }

    public void start() throws IOException {
        ensureDir(baseDir);

        final AtomicBoolean running = new AtomicBoolean(true);

        try (ServerSocket server = new ServerSocket(port);
             DynamicThreadPool pool = new DynamicThreadPool(NMIN, NMAX, EXTRA_IDLE_TIMEOUT_MS)) {

            System.out.println("TCP server started (Thread Pool + multi-acceptor)");
            System.out.println("Port: " + port);
            System.out.println("Storage: " + baseDir.getAbsolutePath());
            System.out.println("Pool: Nmin=" + NMIN + ", Nmax=" + NMAX +
                    ", extraIdleTimeoutMs=" + EXTRA_IDLE_TIMEOUT_MS);
            System.out.println("Acceptors: " + ACCEPTORS + "\n");

            // чтобы Ctrl+C/kill корректно остановили acceptor-ы
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                running.set(false);
                try { server.close(); } catch (Exception ignored) {}
                try { pool.close(); } catch (Exception ignored) {}
            }, "shutdown"));

            Thread[] acc = new Thread[ACCEPTORS];
            for (int i = 0; i < ACCEPTORS; i++) {
                final int idx = i;
                acc[i] = new Thread(() -> acceptorLoop(idx, server, pool, running), "acceptor-" + idx);
                acc[i].setDaemon(true);
                acc[i].start();
            }

            // main поток просто “живет”, пока сервер работает
            // (можно заменить на join acceptor-ов, но тогда shutdown hook не всегда удобен)
            while (running.get()) {
                try { Thread.sleep(1_000); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
            }
        }
    }

    private void acceptorLoop(int idx, ServerSocket server, DynamicThreadPool pool, AtomicBoolean running) {
        while (running.get()) {
            try {
                final Socket client;
                // критическая секция: защищаем accept
                synchronized (acceptLock) {
                    client = server.accept();
                }

                pool.submit(() -> handleClient(client));

            } catch (IOException e) {
                // server.close() при shutdown кинет SocketException — это нормально
                if (running.get()) {
                    // если надо — можно логировать редкие ошибки accept
                    // System.out.println("Acceptor-" + idx + " accept error: " + e.getMessage());
                }
                return;
            } catch (Exception e) {
                // чтобы acceptor не умирал от неожиданных исключений
                e.printStackTrace();
            }
        }
    }

    private void handleClient(Socket s) {
        try (Socket socket = s) {
            socket.setTcpNoDelay(true);
            socket.setKeepAlive(true);

            InputStream in = new BufferedInputStream(socket.getInputStream(), IO_BUF);
            OutputStream out = new BufferedOutputStream(socket.getOutputStream(), IO_BUF);

            // ВАЖНО: без приветствия (клиент ожидает ответ на команду)

            while (true) {
                String line = readLine(in);
                if (line == null) return; // EOF

                line = line.trim();
                if (line.isEmpty()) continue;

                String[] parts = line.split("\\s+");
                String cmd = parts[0].toUpperCase(Locale.ROOT);

                switch (cmd) {
                    case "ECHO" -> {
                        String payload = line.length() > 4 ? line.substring(4).trim() : "";
                        writeLine(out, "OK " + payload);
                        out.flush();
                    }
                    case "TIME" -> {
                        writeLine(out, "OK " + Instant.now());
                        out.flush();
                    }
                    case "CLOSE", "QUIT", "EXIT" -> {
                        writeLine(out, "BYE");
                        out.flush();
                        return;
                    }
                    case "UPLOAD" -> handleUpload(parts, in, out);
                    case "DOWNLOAD" -> handleDownload(parts, out);
                    default -> {
                        writeLine(out, "ERR Unknown command");
                        out.flush();
                    }
                }
            }
        } catch (IOException ignored) {
            // клиент оборвал соединение — норм
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // UPLOAD <name> <totalSize> <offset>
    private void handleUpload(String[] parts, InputStream in, OutputStream out) throws IOException {
        if (parts.length < 4) {
            writeLine(out, "ERR Usage: UPLOAD <name> <size> <offset>");
            out.flush();
            return;
        }

        String name = sanitizeFileName(parts[1]);
        long totalSize = parseLong(parts[2], -1);
        long offset = parseLong(parts[3], -1);

        if (name == null || totalSize < 0 || offset < 0 || offset > totalSize) {
            writeLine(out, "ERR Bad args");
            out.flush();
            return;
        }

        File target = new File(baseDir, name);
        ensureDir(target.getParentFile());

        long current = target.exists() ? target.length() : 0L;
        if (offset != current) {
            // клиент парсит "Server has X"
            writeLine(out, "ERR Offset mismatch. Server has " + current);
            out.flush();
            return;
        }

        long remaining = totalSize - offset;

        // handshake: OK -> клиент шлёт remaining байт
        writeLine(out, "OK");
        out.flush();

        try (RandomAccessFile raf = new RandomAccessFile(target, "rw")) {
            raf.seek(offset);

            byte[] buf = new byte[IO_BUF];
            long left = remaining;

            while (left > 0) {
                int need = (int) Math.min(buf.length, left);
                int n = readFully(in, buf, 0, need);
                if (n == -1) throw new EOFException("Client EOF during upload, left=" + left);
                raf.write(buf, 0, n);
                left -= n;
            }
        }

        writeLine(out, "OK DONE " + totalSize);
        out.flush();
    }

    // DOWNLOAD <name> <offset>
    private void handleDownload(String[] parts, OutputStream out) throws IOException {
        if (parts.length < 3) {
            writeLine(out, "ERR Usage: DOWNLOAD <name> <offset>");
            out.flush();
            return;
        }

        String name = sanitizeFileName(parts[1]);
        long offset = parseLong(parts[2], -1);

        if (name == null || offset < 0) {
            writeLine(out, "ERR Bad args");
            out.flush();
            return;
        }

        File source = new File(baseDir, name);
        if (!source.exists() || !source.isFile()) {
            writeLine(out, "ERR No such file");
            out.flush();
            return;
        }

        long total = source.length();
        if (offset > total) {
            writeLine(out, "ERR Offset too big");
            out.flush();
            return;
        }

        writeLine(out, "OK " + total);
        out.flush();

        long remaining = total - offset;

        try (RandomAccessFile raf = new RandomAccessFile(source, "r")) {
            raf.seek(offset);

            byte[] buf = new byte[IO_BUF];
            long left = remaining;

            while (left > 0) {
                int r = raf.read(buf, 0, (int) Math.min(buf.length, left));
                if (r == -1) break;
                out.write(buf, 0, r);
                left -= r;
            }
        }

        writeLine(out, "OK DONE " + total);
        out.flush();
    }

    // ===== line I/O =====
    private static String readLine(InputStream in) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream(128);
        while (true) {
            int b = in.read();
            if (b == -1) return bos.size() == 0 ? null : bos.toString(StandardCharsets.UTF_8);
            if (b == '\n') break;
            if (b != '\r') bos.write(b);
            if (bos.size() > MAX_LINE) throw new IOException("Line too long");
        }
        return bos.toString(StandardCharsets.UTF_8);
    }

    private static void writeLine(OutputStream out, String line) throws IOException {
        out.write(line.getBytes(StandardCharsets.UTF_8));
        out.write('\n');
    }

    private static int readFully(InputStream in, byte[] buf, int off, int len) throws IOException {
        int total = 0;
        while (total < len) {
            int n = in.read(buf, off + total, len - total);
            if (n == -1) return total == 0 ? -1 : total;
            total += n;
        }
        return total;
    }

    // ===== utils =====
    private static void ensureDir(File dir) throws IOException {
        if (dir == null) return;
        if (dir.exists()) {
            if (!dir.isDirectory()) throw new IOException("Not a directory: " + dir);
            return;
        }
        if (!dir.mkdirs()) throw new IOException("Cannot create dir: " + dir);
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
        new TcpFileServerPooled(port, dir).start();
    }
}
