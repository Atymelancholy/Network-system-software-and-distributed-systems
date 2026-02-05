package org.example;

import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.time.Instant;

public final class TcpFileServer {
    private static final int DEFAULT_PORT = 50505;
    private static final String DEFAULT_DIR = "server_storage";

    private final int port;
    private final File baseDir;

    public TcpFileServer(int port, File baseDir) {
        this.port = port;
        this.baseDir = baseDir;
    }

    public void start() throws IOException {
        ensureDir(baseDir);
        try (ServerSocket server = new ServerSocket(port)) {
            System.out.println("TCP server started");
            System.out.println("Port: " + port);
            System.out.println("Storage: " + baseDir.getAbsolutePath());
            System.out.println("Waiting for clients...\n");

            while (true) {
                try (Socket socket = server.accept()) {
                    configureSocket(socket);
                    System.out.println("Client: " + socket.getRemoteSocketAddress());
                    handleClient(socket);
                    System.out.println("Client disconnected.\n");
                } catch (SocketTimeoutException e) {
                    System.out.println("Client timeout: " + e.getMessage());
                } catch (Exception e) {
                    System.out.println("Client session error: " + e.getMessage());
                }
            }
        }
    }

    private void handleClient(Socket socket) throws IOException {
        InputStream rawIn = new BufferedInputStream(socket.getInputStream());
        OutputStream rawOut = new BufferedOutputStream(socket.getOutputStream());

        BufferedReader reader = new BufferedReader(new InputStreamReader(rawIn, StandardCharsets.UTF_8));
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(rawOut, StandardCharsets.UTF_8));

        while (true) {
            String line = reader.readLine(); // команды читаем только построчно
            if (line == null) return;

            line = line.trim();
            if (line.isEmpty()) continue;

            String[] parts = line.split("\\s+");
            String cmd = parts[0].toUpperCase();

            switch (cmd) {
                case "ECHO" -> handleEcho(line, writer, rawOut);
                case "TIME" -> handleTime(writer, rawOut);
                case "CLOSE" -> { sendLine(writer, rawOut, "BYE"); return; }
                case "UPLOAD" -> handleUpload(parts, rawIn, writer, rawOut);
                case "DOWNLOAD" -> handleDownload(parts, writer, rawOut);
                default -> sendLine(writer, rawOut, "ERR Unknown command");
            }
        }
    }

    private void handleEcho(String line, BufferedWriter writer, OutputStream out) throws IOException {
        String payload = line.length() > 4 ? line.substring(4).trim() : "";
        sendLine(writer, out, "OK " + payload);
    }

    private void handleTime(BufferedWriter writer, OutputStream out) throws IOException {
        sendLine(writer, out, "OK " + Instant.now());
    }

    // UPLOAD <name> <totalSize> <offset>
    // Далее клиент отправляет totalSize-offset байт
    private void handleUpload(String[] parts, InputStream in, BufferedWriter writer, OutputStream out) throws IOException {
        if (parts.length < 4) {
            sendLine(writer, out, "ERR Usage: UPLOAD <name> <size> <offset>");
            return;
        }

        String name = sanitizeFileName(parts[1]);
        long totalSize = parseLong(parts[2], -1);
        long offset = parseLong(parts[3], -1);

        if (name == null || totalSize < 0 || offset < 0 || offset > totalSize) {
            sendLine(writer, out, "ERR Bad args");
            return;
        }

        File target = new File(baseDir, name);
        ensureDir(target.getParentFile());

        long current = target.exists() ? target.length() : 0L;

        // В лоб, но правильно для докачки: offset должен совпадать с текущим размером.
        if (offset != current) {
            sendLine(writer, out, "ERR Offset mismatch. Server has " + current);
            return;
        }

        sendLine(writer, out, "OK");

        long toRead = totalSize - offset;
        long got = receiveToFile(in, target, offset, toRead);
        if (got == toRead) {
            System.out.println("UPLOAD done: " + name + " total=" + totalSize);
        } else {
            System.out.println("UPLOAD incomplete: " + name + " got=" + got + "/" + toRead);
        }
    }

    // DOWNLOAD <name> <offset>
    // Сервер отвечает: OK <totalSize> и затем отправляет totalSize-offset байт
    private void handleDownload(String[] parts, BufferedWriter writer, OutputStream out) throws IOException {
        if (parts.length < 3) {
            sendLine(writer, out, "ERR Usage: DOWNLOAD <name> <offset>");
            return;
        }

        String name = sanitizeFileName(parts[1]);
        long offset = parseLong(parts[2], -1);
        if (name == null || offset < 0) {
            sendLine(writer, out, "ERR Bad args");
            return;
        }

        File source = new File(baseDir, name);
        if (!source.exists() || !source.isFile()) {
            sendLine(writer, out, "ERR No such file");
            return;
        }

        long total = source.length();
        if (offset > total) {
            sendLine(writer, out, "ERR Offset too big");
            return;
        }

        sendLine(writer, out, "OK " + total);
        sendFile(out, source, offset);
        out.flush();
    }

    // ===== data transfer =====

    private long receiveToFile(InputStream in, File file, long offset, long toRead) throws IOException {
        long startNs = System.nanoTime();

        try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
            raf.seek(offset);

            byte[] buf = new byte[64 * 1024];
            long remaining = toRead;
            long totalRead = 0;

            while (remaining > 0) {
                int want = (int) Math.min(buf.length, remaining);
                int n = in.read(buf, 0, want);
                if (n == -1) break;
                raf.write(buf, 0, n);
                remaining -= n;
                totalRead += n;
            }

            printBitrate("UPLOAD bitrate", totalRead, startNs);
            return totalRead;
        }
    }

    private void sendFile(OutputStream out, File file, long offset) throws IOException {
        long startNs = System.nanoTime();

        try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
            raf.seek(offset);

            byte[] buf = new byte[64 * 1024];
            long totalSent = 0;

            while (true) {
                int n = raf.read(buf);
                if (n == -1) break;
                out.write(buf, 0, n);
                totalSent += n;
            }

            printBitrate("DOWNLOAD bitrate", totalSent, startNs);
        }
    }

    // ===== io helpers =====

    private void sendLine(BufferedWriter writer, OutputStream out, String line) throws IOException {
        writer.write(line);
        writer.write("\n");
        writer.flush();
        out.flush();
    }

    private void configureSocket(Socket socket) throws SocketException {
        socket.setKeepAlive(true);   // SO_KEEPALIVE
        socket.setSoTimeout(120_000); // чтобы не висеть бесконечно
        socket.setTcpNoDelay(true);   // для команд удобно
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
        // запрещаем пути, оставляем только имя файла в текущей папке
        if (raw.contains("..") || raw.startsWith("/") || raw.contains("/")) return null;
        return raw;
    }

    private long parseLong(String s, long def) {
        try { return Long.parseLong(s); } catch (Exception e) { return def; }
    }

    private void printBitrate(String label, long bytes, long startNs) {
        double sec = (System.nanoTime() - startNs) / 1_000_000_000.0;
        if (sec <= 0.000001) return;
        double mbit = (bytes * 8.0) / 1_000_000.0;
        System.out.printf("%s: %.2f Mbit/s (%d bytes in %.3f s)%n", label, mbit / sec, bytes, sec);
    }

    public static void main(String[] args) throws Exception {
        int port = args.length > 0 ? Integer.parseInt(args[0]) : DEFAULT_PORT;
        File dir = new File(args.length > 1 ? args[1] : DEFAULT_DIR);
        new TcpFileServer(port, dir).start();
    }
}

