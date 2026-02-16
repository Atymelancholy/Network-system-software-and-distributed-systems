package org.example;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public final class TcpFileServerPooled {

    private static final int DEFAULT_PORT = 50505;
    private static final String DEFAULT_DIR = "server_storage";

    // === ЛР4: пул потоков ===
    private static final int NMIN = 4;
    private static final int NMAX = 64;
    private static final long EXTRA_IDLE_TIMEOUT_MS = 10_000;

    private static final int IO_BUF = 64 * 1024;
    private static final int MAX_LINE = 8 * 1024;

    private final int port;
    private final File baseDir;

    private final AtomicInteger accepted = new AtomicInteger(0);
    private final AtomicInteger activeClients = new AtomicInteger(0);
    private final AtomicInteger activeUploads = new AtomicInteger(0);
    private final AtomicInteger activeDownloads = new AtomicInteger(0);

    private static void log(String msg) {
        System.out.printf("[%s] %s%n", Thread.currentThread().getName(), msg);
    }

    public TcpFileServerPooled(int port, File baseDir) {
        this.port = port;
        this.baseDir = baseDir;
    }

    public void start() throws IOException {
        ensureDir(baseDir);

        final AtomicBoolean running = new AtomicBoolean(true);

        try (ServerSocket server = new ServerSocket(port);
             DynamicThreadPool pool = new DynamicThreadPool(NMIN, NMAX, EXTRA_IDLE_TIMEOUT_MS)) {

            System.out.println("TCP server started (Thread Pool)");
            System.out.println("Port: " + port);
            System.out.println("Storage: " + baseDir.getAbsolutePath());
            System.out.println("Pool: Nmin=" + NMIN + ", Nmax=" + NMAX +
                    ", extraIdleTimeoutMs=" + EXTRA_IDLE_TIMEOUT_MS + "\n");

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                running.set(false);
                try { server.close(); } catch (Exception ignored) {}
                try { pool.close(); } catch (Exception ignored) {}
            }, "shutdown"));

            // ✅ 1 accept loop => взаимное исключение "само собой"
            while (running.get()) {
                try {
                    Socket client = server.accept();
                    int id = accepted.incrementAndGet();

                    log("ACCEPT conn#" + id + " from " + client.getRemoteSocketAddress());

                    final int connId = id;
                    pool.submit(() -> handleClient(connId, client));
                } catch (IOException e) {
                    if (running.get()) log("accept error: " + e.getMessage());
                    return;
                }
            }
        }
    }

    private void handleClient(int connId, Socket s) {
        int nowActive = activeClients.incrementAndGet();
        log("START client #" + connId + " (activeClients=" + nowActive + ")");

        try (Socket socket = s) {
            socket.setTcpNoDelay(true);
            socket.setKeepAlive(true);

            InputStream in = new BufferedInputStream(socket.getInputStream(), IO_BUF);
            OutputStream out = new BufferedOutputStream(socket.getOutputStream(), IO_BUF);

            while (true) {
                String line = readLine(in);
                if (line == null) return;

                line = line.trim();
                if (line.isEmpty()) continue;

                log("CMD from #" + connId + ": " + line);

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
                    case "UPLOAD" -> handleUpload(connId, parts, in, out);
                    case "DOWNLOAD" -> handleDownload(connId, parts, out);
                    default -> {
                        writeLine(out, "ERR Unknown command");
                        out.flush();
                    }
                }
            }
        } catch (IOException ignored) {
            log("IO disconnect client #" + connId);
        } catch (Exception e) {
            log("ERROR client #" + connId + ": " + e.getMessage());
            e.printStackTrace();
        } finally {
            int left = activeClients.decrementAndGet();
            log("END client #" + connId + " (activeClients=" + left + ")");
        }
    }

    // UPLOAD <name> <totalSize> <offset>
    private void handleUpload(int connId, String[] parts, InputStream in, OutputStream out) throws IOException {
        int upNow = activeUploads.incrementAndGet();
        log("UPLOAD start conn#" + connId + " (activeUploads=" + upNow + ")");

        try {
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
                writeLine(out, "ERR Offset mismatch. Server has " + current);
                out.flush();
                log("UPLOAD reject conn#" + connId + " offset=" + offset + " serverHas=" + current);
                return;
            }

            long remaining = totalSize - offset;

            writeLine(out, "OK");
            out.flush();

            long startNs = System.nanoTime();

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

            double sec = (System.nanoTime() - startNs) / 1_000_000_000.0;
            log("UPLOAD done conn#" + connId + " bytes=" + remaining + " time=" + String.format(Locale.ROOT, "%.3f", sec) + "s");

            writeLine(out, "OK DONE " + totalSize);
            out.flush();
        } finally {
            int left = activeUploads.decrementAndGet();
            log("UPLOAD end conn#" + connId + " (activeUploads=" + left + ")");
        }
    }

    // DOWNLOAD <name> <offset>
    private void handleDownload(int connId, String[] parts, OutputStream out) throws IOException {
        int dlNow = activeDownloads.incrementAndGet();
        log("DOWNLOAD start conn#" + connId + " (activeDownloads=" + dlNow + ")");

        try {
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
            long startNs = System.nanoTime();

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

            out.flush();

            double sec = (System.nanoTime() - startNs) / 1_000_000_000.0;
            log("DOWNLOAD sent conn#" + connId + " bytes=" + remaining + " time=" + String.format(Locale.ROOT, "%.3f", sec) + "s");

            writeLine(out, "OK DONE " + total);
            out.flush();
        } finally {
            int left = activeDownloads.decrementAndGet();
            log("DOWNLOAD end conn#" + connId + " (activeDownloads=" + left + ")");
        }
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
