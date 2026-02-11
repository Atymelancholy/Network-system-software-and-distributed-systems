package org.example;

import java.io.*;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.net.InetSocketAddress;

public final class TcpFileClient {

    private static final long NO_PROGRESS_LIMIT_MS = 60_000;
    // обнаружение в разумное время (30 сек)
    private static final long AUTO_RECOVERY_WINDOW_MS = 90_000;   // автопопытки ДО сообщения (например 90 сек)
    private static final long RETRY_DELAY_MS = 5_000;
    private static final int CONNECT_TIMEOUT_MS = 30_000;
    private static final int READ_TIMEOUT_MS = 30_000;
    // пауза между переподключениями
    private static final int MAX_LINE = 8 * 1024;

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            printUsageAndExit();
            return;
        }
        String host = args[0];
        int port = Integer.parseInt(args[1]);
        String cmd = args.length >= 3 ? args[2].toLowerCase() : "";
        switch (cmd) {
            case "time" -> runSimple(host, port, "TIME");
            case "echo" -> runSimple(host, port, "ECHO " + joinFrom(args, 3));
            case "close" -> runSimple(host, port, "CLOSE");
            case "upload" -> {
                if (args.length < 5) {
                    printUsageAndExit();
                    return;
                }
                upload(host, port, new File(args[3]), args[4]);
            }
            case "download" -> {
                if (args.length < 5) {
                    printUsageAndExit();
                    return;
                }
                download(host, port, args[3], new File(args[4]));
            }
            default -> {
                printUsageAndExit();
                return;
            }
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
        Example:
            java TcpFileClient 192.168.1.10 50505 time
            java TcpFileClient 192.168.1.10 50505 echo hello world
            java TcpFileClient 192.168.1.10 50505 close
            java TcpFileClient 192.168.1.10 50505 upload ./big.bin big.bin
            java TcpFileClient 192.168.1.10 50505 download big.bin ./big.bin
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

        long deadline = System.currentTimeMillis() + AUTO_RECOVERY_WINDOW_MS;

        while (true) {
            try (Session s = Session.connect(host, port)) {
                String req = "UPLOAD " + remoteName + " " + totalSize + " " + offset;
                s.sendLine(req);

                String resp = s.readLine();
                if (resp == null) throw new IOException("Disconnected during handshake");

                if (resp.startsWith("OK")) {
                    deadline = System.currentTimeMillis() + AUTO_RECOVERY_WINDOW_MS;

                    long sent = sendFileFromOffset(s.out, localFile, offset);
                    s.out.flush();

                    offset += sent; // ✅ ВАЖНО: учитываем отправленное (для корректного resume после обрыва)

                    String finalResp = s.readLine();
                    if (finalResp == null) throw new IOException("Disconnected while waiting server confirmation");

                    if (finalResp.startsWith("OK DONE")) {
                        System.out.println("UPLOAD finished. Sent bytes: " + sent);
                        return;
                    }

                    Long serverHas = parseServerHas(finalResp);
                    if (serverHas != null && serverHas >= 0 && serverHas <= totalSize) {
                        offset = serverHas;
                        continue;
                    }

                    throw new IOException("Upload not confirmed: " + finalResp);
                }

                Long serverHas = parseServerHas(resp);
                if (serverHas != null && serverHas >= 0 && serverHas <= totalSize) {
                    offset = serverHas;
                    continue;
                }

                throw new IOException("Server error: " + resp);

            } catch (SocketTimeoutException | SocketException e) {
                if (System.currentTimeMillis() < deadline) {
                    sleepSilently(RETRY_DELAY_MS);
                    continue;
                }
                System.out.println("Connection problem during UPLOAD. Automatic recovery failed within "
                        + (AUTO_RECOVERY_WINDOW_MS / 1000) + " seconds. Retry upload command to resume.");
                return;

            } catch (IOException e) {
                if (System.currentTimeMillis() < deadline) {
                    sleepSilently(RETRY_DELAY_MS);
                    continue;
                }
                System.out.println("Connection problem during UPLOAD: " + e.getMessage()
                        + ". Automatic recovery failed within " + (AUTO_RECOVERY_WINDOW_MS / 1000)
                        + " seconds. Retry upload command to resume.");
                return;
            }
        }
    }

    private static void download(String host, int port, String remoteName, File localFile) throws IOException {
        long deadline = System.currentTimeMillis() + AUTO_RECOVERY_WINDOW_MS;

        while (true) {
            long offset = localFile.exists() ? localFile.length() : 0L;

            try (Session s = Session.connect(host, port)) {
                String req = "DOWNLOAD " + remoteName + " " + offset;
                System.out.println("DOWNLOAD request: name=" + remoteName + " localPath=" + localFile.getPath()
                        + " offset=" + offset + " (localSize=" + offset + ")");

                s.sendLine(req);

                String resp = s.readLine();
                if (resp == null) throw new IOException("Disconnected during handshake");
                if (!resp.startsWith("OK")) throw new IOException("Server error: " + resp);

                deadline = System.currentTimeMillis() + AUTO_RECOVERY_WINDOW_MS; // ✅ прогресс есть → окно автопочинки продлеваем

                long total = Long.parseLong(resp.split("\\s+")[1]);
                if (offset > total) throw new IOException("Local file bigger than server file; delete local and retry");

                long toRead = total - offset;
                long got = receiveToFileWithProgress(s, localFile, offset, toRead);

                if (got > 0) {
                    deadline = System.currentTimeMillis() + AUTO_RECOVERY_WINDOW_MS; // ✅
                }

                if (got == toRead) {
                    // ✅ дочитали байты — теперь ждём подтверждение конца
                    String done = s.readLine();
                    if (done == null || !done.startsWith("OK DONE")) {
                        throw new IOException("Server did not confirm DONE: " + done);
                    }

                    System.out.println("DOWNLOAD finished. Got bytes: " + got + " / " + toRead);
                    return;
                }


                // если не дочитали — считаем это проблемой канала
                throw new IOException("Disconnected during download stream");

            } catch (SocketTimeoutException | SocketException e) {
                if (System.currentTimeMillis() < deadline) {
                    sleepSilently(RETRY_DELAY_MS);
                    continue;
                }
                System.out.println("Connection problem during DOWNLOAD. Automatic recovery failed within "
                        + (AUTO_RECOVERY_WINDOW_MS / 1000) + " seconds. Retry download command to resume.");
                return;

            } catch (IOException e) {
                if (System.currentTimeMillis() < deadline) {
                    sleepSilently(RETRY_DELAY_MS);
                    continue;
                }
                System.out.println("Connection problem during DOWNLOAD: " + e.getMessage()
                        + ". Automatic recovery failed within " + (AUTO_RECOVERY_WINDOW_MS / 1000)
                        + " seconds. Retry download command to resume.");
                return;
            }
        }
    }

    private static long receiveToFileWithProgress(Session s, File file, long offset, long toRead) throws IOException {
        long startNs = System.nanoTime();
        long lastProgress = System.currentTimeMillis();

        try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
            raf.seek(offset);
            byte[] buf = new byte[64 * 1024];
            long remaining = toRead;
            long totalRead = 0;

            while (remaining > 0) {
                int want = (int) Math.min(buf.length, remaining);
                try {
                    int n = s.in.read(buf, 0, want);
                    if (n == -1) {
                        throw new EOFException("EOF during download stream, remaining=" + remaining);
                    }
                    raf.write(buf, 0, n);
                    remaining -= n;
                    totalRead += n;
                    lastProgress = System.currentTimeMillis();
                } catch (SocketTimeoutException te) {
                    // просто нет данных сейчас — проверяем, сколько уже ждём без прогресса
                    long stall = System.currentTimeMillis() - lastProgress;
                    if (stall >= NO_PROGRESS_LIMIT_MS) {
                        System.out.println("DOWNLOAD: no data for " + stall + " ms -> reconnect/resume");
                        throw te;
                    }
                    // иначе ждём дальше
                }
            }

            printBitrate("DOWNLOAD bitrate", totalRead, startNs);
            return totalRead;
        }
    }

    // ===== transfer helpers =====
    private static void sleepSilently(long ms) {
        try { Thread.sleep(ms); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
    }
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
    private static void printBitrate(String label, long bytes, long startNs) {
        double sec = (System.nanoTime() - startNs) / 1_000_000_000.0;
        if (sec <= 0.000001) return;
        double mbit = (bytes * 8.0) / 1_000_000.0;
        System.out.printf("%s: %.2f Mbit/s (%d bytes in %.3f s)%n", label, mbit / sec, bytes, sec);
    }
    private static Long parseServerHas(String resp) {
        String marker = "Server has ";
        int i = resp.indexOf(marker);
        if (i < 0) return null;

        String tail = resp.substring(i + marker.length()).trim();
        int sp = tail.indexOf(' ');
        if (sp >= 0) tail = tail.substring(0, sp);

        try { return Long.parseLong(tail); }
        catch (Exception e) { return null; }
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
    static String readLine(InputStream in) throws IOException {
        ByteArrayOutputStream buf = new ByteArrayOutputStream(128);
        while (true) {
            int b = in.read();
            if (b == -1) return buf.size() == 0 ? null : buf.toString(StandardCharsets.UTF_8);
            if (b == '\n') break;
            if (b != '\r') buf.write(b);
            if (buf.size() > MAX_LINE) throw new IOException("Line too long");
        }
        return buf.toString(StandardCharsets.UTF_8);
    }
    static void sendLine(OutputStream out, String line) throws IOException {
        out.write(line.getBytes(StandardCharsets.UTF_8));
        out.write('\n');
        out.flush();
    }
    // ===== Session wrapper =====
    private static final class Session implements Closeable {
        final Socket socket;
        final InputStream in;
        final OutputStream out;

        private Session(Socket socket) throws IOException {
            this.socket = socket;
            this.in = new BufferedInputStream(socket.getInputStream());
            this.out = new BufferedOutputStream(socket.getOutputStream());
        }

        static Session connect(String host, int port) throws IOException {
            Socket s = new Socket();
            s.setKeepAlive(true);
            s.setTcpNoDelay(true);

            s.connect(new InetSocketAddress(host, port), CONNECT_TIMEOUT_MS); // ✅ отдельный таймаут на connect
            s.setSoTimeout(READ_TIMEOUT_MS);                                  // ✅ отдельный таймаут на read
            return new Session(s);
        }

        void sendLine(String line) throws IOException {
            TcpFileClient.sendLine(out, line);
        }

        String readLine() throws IOException {
            return TcpFileClient.readLine(in);
        }

        String sendLineAndReadLine(String line) throws IOException {
            sendLine(line);
            return readLine();
        }

        @Override public void close() {
            try { socket.close(); } catch (Exception ignored) {}
        }
    }
}