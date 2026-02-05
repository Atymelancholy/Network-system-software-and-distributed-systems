package org.example;

import java.io.*;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicBoolean;

public final class TcpFileClient {
    private static final int DEFAULT_PORT = 50505;
    private static final int CONNECTION_TIMEOUT_MS = 30000; // 30 секунд для обнаружения разрыва
    private static final int RECOVERY_TIMEOUT_MS = 120000; // 2 минуты для восстановления

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
            case "exit" -> runSimple(host, port, "EXIT");
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
              java TcpFileClient <host> <port> exit
              java TcpFileClient <host> <port> upload <localPath> <remoteName>
              java TcpFileClient <host> <port> download <remoteName> <localPath>
            Example:
              java TcpFileClient 192.168.1.10 50505 time
              java TcpFileClient 192.168.1.10 50505 echo hello world
              java TcpFileClient 192.168.1.10 50505 close
              java TcpFileClient 192.168.1.10 50505 upload ./big.bin big.bin
              java TcpFileClient 192.168.1.10 50505 download big.bin ./big.bin
            """);
        System.exit(1);
    }

    private static void runSimple(String host, int port, String command) throws IOException {
        try (Session s = Session.connect(host, port)) {
            String resp = s.sendLineAndReadLine(command);
            System.out.println(resp == null ? "<no response>" : resp);
        }
    }

    private static void upload(String host, int port, File localFile, String remoteName) throws IOException {
        if (!localFile.exists() || !localFile.isFile()) {
            throw new FileNotFoundException("Local file not found: " + localFile.getPath());
        }

        long totalSize = localFile.length();
        long offset = 0;
        int attempt = 0;
        final int MAX_ATTEMPTS = 3;

        while (true) {
            attempt++;
            try (Session s = Session.connect(host, port)) {
                AtomicBoolean connectionHealthy = new AtomicBoolean(true);

                // Монитор соединения
                Thread connectionMonitor = new Thread(() -> {
                    try {
                        Thread.sleep(CONNECTION_TIMEOUT_MS);
                        if (connectionHealthy.get()) {
                            System.out.println("Connection monitoring: connection appears healthy");
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                });
                connectionMonitor.start();

                String req = "UPLOAD " + remoteName + " " + totalSize + " " + offset;
                s.sendLine(req);
                String resp = s.readLine();

                if (resp == null) {
                    connectionHealthy.set(false);
                    handleConnectionIssue("Disconnected during handshake", attempt, MAX_ATTEMPTS);
                    continue;
                }

                if (resp.startsWith("OK")) {
                    System.out.println("Starting upload from offset " + offset + " of " + totalSize);
                    long sent = sendFileFromOffset(s.out, localFile, offset, connectionHealthy);
                    s.out.flush();

                    if (connectionHealthy.get()) {
                        System.out.println("UPLOAD completed successfully. Sent bytes: " + sent);
                        return;
                    } else {
                        System.out.println("UPLOAD interrupted. Sent " + sent + " bytes.");
                        handleConnectionIssue("Upload interrupted", attempt, MAX_ATTEMPTS);
                        offset += sent;
                        continue;
                    }
                }

                // Автодокачка
                Long serverHas = parseServerHas(resp);
                if (serverHas != null && serverHas >= 0 && serverHas <= totalSize) {
                    System.out.println("Server has " + serverHas + " bytes already. Resuming...");
                    offset = serverHas;
                    continue;
                }

                throw new IOException("Server error: " + resp);

            } catch (IOException e) {
                System.err.println("Attempt " + attempt + " failed: " + e.getMessage());
                if (attempt >= MAX_ATTEMPTS) {
                    System.err.println("Max attempts reached. Manual recovery required.");
                    throw e;
                }

                System.out.println("Waiting " + (RECOVERY_TIMEOUT_MS/1000) + " seconds before retry...");
                try {
                    Thread.sleep(RECOVERY_TIMEOUT_MS);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new IOException("Interrupted during recovery", ie);
                }
            }
        }
    }

    private static void download(String host, int port, String remoteName, File localFile) throws IOException {
        long offset = localFile.exists() ? localFile.length() : 0L;
        int attempt = 0;
        final int MAX_ATTEMPTS = 3;

        while (true) {
            attempt++;
            try (Session s = Session.connect(host, port)) {
                AtomicBoolean connectionHealthy = new AtomicBoolean(true);

                // Монитор соединения
                Thread connectionMonitor = new Thread(() -> {
                    try {
                        Thread.sleep(CONNECTION_TIMEOUT_MS);
                        if (connectionHealthy.get()) {
                            System.out.println("Connection monitoring: connection appears healthy");
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                });
                connectionMonitor.start();

                String req = "DOWNLOAD " + remoteName + " " + offset;
                s.sendLine(req);
                String resp = s.readLine();

                if (resp == null) {
                    connectionHealthy.set(false);
                    handleConnectionIssue("Disconnected during handshake", attempt, MAX_ATTEMPTS);
                    continue;
                }

                if (!resp.startsWith("OK")) {
                    throw new IOException("Server error: " + resp);
                }

                long total = Long.parseLong(resp.split("\\s+")[1]);
                if (offset > total) {
                    throw new IOException("Local file bigger than server file; delete local and retry");
                }

                long toRead = total - offset;
                System.out.println("Starting download from offset " + offset + " of " + total);
                long got = receiveToFile(s.in, localFile, offset, toRead, connectionHealthy);

                if (connectionHealthy.get()) {
                    System.out.println("DOWNLOAD completed successfully. Got bytes: " + got + " / " + toRead);
                    return;
                } else {
                    System.out.println("DOWNLOAD interrupted. Got " + got + " bytes.");
                    handleConnectionIssue("Download interrupted", attempt, MAX_ATTEMPTS);
                    offset += got;
                    continue;
                }

            } catch (IOException e) {
                System.err.println("Attempt " + attempt + " failed: " + e.getMessage());
                if (attempt >= MAX_ATTEMPTS) {
                    System.err.println("Max attempts reached. Manual recovery required.");
                    throw e;
                }

                System.out.println("Waiting " + (RECOVERY_TIMEOUT_MS/1000) + " seconds before retry...");
                try {
                    Thread.sleep(RECOVERY_TIMEOUT_MS);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new IOException("Interrupted during recovery", ie);
                }
            }
        }
    }

    private static void handleConnectionIssue(String message, int attempt, int maxAttempts) {
        System.err.println("CONNECTION ISSUE: " + message);
        System.err.println("Attempt " + attempt + " of " + maxAttempts);

        if (attempt < maxAttempts) {
            System.err.println("Will attempt automatic recovery...");
        } else {
            System.err.println("Maximum automatic recovery attempts reached.");
            System.err.println("Please check network connection and retry manually.");
        }
    }

    private static long sendFileFromOffset(OutputStream out, File file, long offset,
                                           AtomicBoolean connectionHealthy) throws IOException {
        long startNs = System.nanoTime();
        try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
            raf.seek(offset);
            byte[] buf = new byte[64 * 1024];
            long totalSent = 0;
            long fileSize = file.length() - offset;
            long lastActivityTime = System.currentTimeMillis();

            while (totalSent < fileSize && connectionHealthy.get()) {
                // Проверка таймаута
                if (System.currentTimeMillis() - lastActivityTime > CONNECTION_TIMEOUT_MS) {
                    System.out.println("Connection timeout during file send");
                    connectionHealthy.set(false);
                    break;
                }

                int toSend = (int) Math.min(buf.length, fileSize - totalSent);
                int n = raf.read(buf, 0, toSend);
                if (n == -1) break;

                out.write(buf, 0, n);
                totalSent += n;
                lastActivityTime = System.currentTimeMillis();

                // Периодический прогресс
                if (totalSent % (64 * 1024 * 10) == 0) {
                    System.out.println("Send progress: " + totalSent + "/" + fileSize + " bytes");
                }
            }

            printBitrate("UPLOAD bitrate", totalSent, startNs);
            return totalSent;
        }
    }

    private static long receiveToFile(InputStream in, File file, long offset, long toRead,
                                      AtomicBoolean connectionHealthy) throws IOException {
        long startNs = System.nanoTime();
        try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
            raf.seek(offset);
            byte[] buf = new byte[64 * 1024];
            long remaining = toRead;
            long totalRead = 0;
            long lastActivityTime = System.currentTimeMillis();

            while (remaining > 0 && connectionHealthy.get()) {
                // Проверка таймаута
                if (System.currentTimeMillis() - lastActivityTime > CONNECTION_TIMEOUT_MS) {
                    System.out.println("Connection timeout during file receive");
                    connectionHealthy.set(false);
                    break;
                }

                int want = (int) Math.min(buf.length, remaining);
                int n = in.read(buf, 0, want);
                if (n == -1) {
                    System.out.println("End of stream during file receive");
                    connectionHealthy.set(false);
                    break;
                }

                raf.write(buf, 0, n);
                remaining -= n;
                totalRead += n;
                lastActivityTime = System.currentTimeMillis();

                // Периодический прогресс
                if (totalRead % (64 * 1024 * 10) == 0) {
                    System.out.println("Receive progress: " + totalRead + "/" + toRead + " bytes");
                }
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
        String marker = "Server has ";
        int i = resp.indexOf(marker);
        if (i < 0) return null;
        String tail = resp.substring(i + marker.length()).trim();
        try {
            return Long.parseLong(tail);
        } catch (Exception e) {
            return null;
        }
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
            s.setKeepAlive(true);
            s.setSoTimeout(CONNECTION_TIMEOUT_MS);
            s.setTcpNoDelay(true);
            System.out.println("Connected to " + host + ":" + port);
            System.out.println("SO_KEEPALIVE: " + s.getKeepAlive());
            System.out.println("SO_TIMEOUT: " + s.getSoTimeout() + "ms");
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

        @Override
        public void close() throws IOException {
            try {
                socket.close();
                System.out.println("Connection closed");
            } catch (Exception ignored) {}
        }
    }
}
