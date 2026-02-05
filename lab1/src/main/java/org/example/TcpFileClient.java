package org.example;

import java.io.*;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;

public final class TcpFileClient {
    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            printUsageAndExit();
        }
        String host = args[0];
        int port = Integer.parseInt(args[1]);
        String cmd = args.length >= 3 ? args[2].toLowerCase() : "";
        switch (cmd) {
            case "time" -> runSimple(host, port, "TIME");
            case "echo" -> runSimple(host, port, "ECHO " + joinFrom(args, 3));
            case "close" -> runSimple(host, port, "CLOSE");
            case "upload" -> {
                if (args.length < 5) printUsageAndExit();
                upload(host, port, new File(args[3]), args[4]);
            }
            case "download" -> {
                if (args.length < 5) printUsageAndExit();
                download(host, port, args[3], new File(args[4]));
            }
            default -> printUsageAndExit();
        }
    }
    private static void printUsageAndExit() {
        System.out.println("""
        Usage:
          java TcpFileClient <host> <port> time
          java TcpFileClient <host> <port> echo <text...>
          java TcpFileClient <host> <port> close
          java TcpFileClient <host> <port> upload <localPath> <remoteName>
          java TcpFileClient <host> <port> download <remoteName> <localPath>
        """);
        System.exit(1);   // ← ВОТ ЭТА СТРОКА ГЛАВНАЯ
    }
    private static void runSimple(String host, int port, String command) {
        try (Session s = Session.connect(host, port)) {
            String resp = s.sendLineAndReadLine(command);
            System.out.println(resp == null ? "Connection lost (no response)" : resp);
        } catch (SocketTimeoutException e) {
            System.out.println("Connection problem: timeout (server not responding).");
        } catch (IOException e) {
            System.out.println("Connection problem: " + e.getMessage());
        }
    }

    private static void upload(String host, int port, File localFile, String remoteName) throws IOException {
        if (!localFile.exists() || !localFile.isFile()) {
            throw new FileNotFoundException("Local file not found: " + localFile.getPath());
        }

        long totalSize = localFile.length();
        long offset = 0;

        while (true) {
            try (Session s = Session.connect(host, port)) {
                String req = "UPLOAD " + remoteName + " " + totalSize + " " + offset;
                s.sendLine(req);

                String resp = s.readLine();
                if (resp == null) throw new IOException("Disconnected during handshake");

                if (resp.startsWith("OK")) {
                    long sent = sendFileFromOffset(s.out, localFile, offset);
                    s.out.flush();
                    System.out.println("UPLOAD finished. Sent bytes: " + sent);
                    return;
                }

                Long serverHas = parseServerHas(resp);
                if (serverHas != null && serverHas >= 0 && serverHas <= totalSize) {
                    System.out.println("Server has " + serverHas + " bytes already. Resuming...");
                    offset = serverHas;
                    continue;
                }

                throw new IOException("Server error: " + resp);

            } catch (SocketTimeoutException e) {
                System.out.println("Connection lost: timeout. Retry upload to resume.");
                return; // по ТЗ: пользователь решает, продолжать ли после сообщения
            } catch (IOException e) {
                System.out.println("Connection lost: " + e.getMessage() + ". Retry upload to resume.");
                return; // то же самое
            }
        }
    }

    private static void download(String host, int port, String remoteName, File localFile) throws IOException {
        long offset = localFile.exists() ? localFile.length() : 0L;

        try (Session s = Session.connect(host, port)) {
            String req = "DOWNLOAD " + remoteName + " " + offset;
            s.sendLine(req);

            String resp = s.readLine();
            if (resp == null) throw new IOException("Disconnected during handshake");

            if (!resp.startsWith("OK")) throw new IOException("Server error: " + resp);

            long total = Long.parseLong(resp.split("\\s+")[1]);
            if (offset > total) throw new IOException("Local file bigger than server file; delete local and retry");

            long toRead = total - offset;
            long got = receiveToFile(s.in, localFile, offset, toRead);

            if (got != toRead) {
                System.out.println("Connection lost during download. Retry download to resume.");
                return;
            }

            System.out.println("DOWNLOAD finished. Got bytes: " + got + " / " + toRead);

        } catch (SocketTimeoutException e) {
            System.out.println("Connection lost: timeout. Retry download to resume.");
        } catch (IOException e) {
            System.out.println("Connection lost: " + e.getMessage() + ". Retry download to resume.");
        }
    }
    // ===== transfer helpers =====
    private static long sendFileFromOffset(OutputStream out, File file, long offset) throws IOException {
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
            printBitrate("UPLOAD bitrate", totalSent, startNs);
            return totalSent;
        }
    }
    private static long receiveToFile(InputStream in, File file, long offset, long toRead) throws IOException {
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
            printBitrate("DOWNLOAD bitrate", totalRead, startNs);
            return totalRead;
        }
    }
    private static void printBitrate(String label, long bytes, long startNs) {
        double sec = (System.nanoTime() - startNs) / 1_000_000_000.0;
        if (sec <= 0.000001) return;
        double mbit = (bytes * 8.0) / 1_000_000.0;
        System.out.printf("%s: %.2f Mbit/s (%d bytes in %.3f s)%n", label, mbit / sec, bytes, sec);
    }
    private static Long parseServerHas(String resp) {
        // ожидаем "ERR Offset mismatch. Server has X"
        String marker = "Server has ";
        int i = resp.indexOf(marker);
        if (i < 0) return null;
        String tail = resp.substring(i + marker.length()).trim();
        try { return Long.parseLong(tail); } catch (Exception e) { return null; }
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
    // ===== Session wrapper =====
    private static final class Session implements Closeable {
        final Socket socket;
        final InputStream in;
        final OutputStream out;
        final BufferedReader reader;
        final BufferedWriter writer;
        private Session(Socket socket) throws IOException {
            this.socket = socket;
            this.in = new BufferedInputStream(socket.getInputStream());
            this.out = new BufferedOutputStream(socket.getOutputStream());
            this.reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8));
            this.writer = new BufferedWriter(new OutputStreamWriter(out, StandardCharsets.UTF_8));
        }
        static Session connect(String host, int port) throws IOException {
            Socket s = new Socket(host, port);
            s.setKeepAlive(true);        // SO_KEEPALIVE
            s.setSoTimeout(120_000);
            s.setTcpNoDelay(true);
            return new Session(s);
        }
        void sendLine(String line) throws IOException {
            writer.write(line);
            writer.write("\n");
            writer.flush();
            out.flush();
        }
        String readLine() throws IOException {
            return reader.readLine();
        }
        String sendLineAndReadLine(String line) throws IOException {
            sendLine(line);
            return readLine();
        }
        @Override public void close() throws IOException {
            try { socket.close(); } catch (Exception ignored) {}
        }
    }
}