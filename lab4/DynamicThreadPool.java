package org.example;

import java.util.ArrayDeque;
import java.util.Deque;

public final class DynamicThreadPool implements AutoCloseable {

    private final int nMin;
    private final int nMax;
    private final long idleTimeoutMs;

    private final Object lock = new Object();
    private final Deque<Runnable> queue = new ArrayDeque<>();

    private int totalWorkers = 0;
    private int idleWorkers = 0;
    private boolean closed = false;

    public DynamicThreadPool(int nMin, int nMax, long idleTimeoutMs) {
        if (nMin <= 0) throw new IllegalArgumentException("nMin must be > 0");
        if (nMax < nMin) throw new IllegalArgumentException("nMax must be >= nMin");
        if (idleTimeoutMs < 0) throw new IllegalArgumentException("idleTimeoutMs must be >= 0");

        this.nMin = nMin;
        this.nMax = nMax;
        this.idleTimeoutMs = idleTimeoutMs;

        for (int i = 0; i < nMin; i++) startWorker(true);
    }

    public void submit(Runnable task) {
        if (task == null) throw new NullPointerException("task");
        synchronized (lock) {
            if (closed) throw new IllegalStateException("pool is closed");
            queue.addLast(task);

            // если нет простаивающих и можем расшириться — создаём extra worker
            if (idleWorkers == 0 && totalWorkers < nMax) {
                startWorker(false);
            }
            lock.notifyAll();
        }
    }

    private void startWorker(boolean core) {
        totalWorkers++;
        Thread t = new Thread(() -> workerLoop(core), core ? "pool-core" : "pool-extra");
        t.setDaemon(true);
        t.start();
    }

    private void workerLoop(boolean core) {
        while (true) {
            Runnable task;

            synchronized (lock) {
                while (!closed && queue.isEmpty()) {
                    idleWorkers++;
                    try {
                        if (core) {
                            lock.wait();
                        } else {
                            lock.wait(idleTimeoutMs);
                            if (!closed && queue.isEmpty() && totalWorkers > nMin) {
                                totalWorkers--;
                                idleWorkers--;
                                return;
                            }
                        }
                    } catch (InterruptedException ignored) {
                        if (closed) {
                            totalWorkers--;
                            idleWorkers--;
                            return;
                        }
                    } finally {
                        // проснулись — уменьшаем idle
                    }
                    idleWorkers--;
                }

                if (closed && queue.isEmpty()) {
                    totalWorkers--;
                    return;
                }

                task = queue.removeFirst();
            }

            try {
                task.run();
            } catch (Throwable t) {
                t.printStackTrace();
            }
        }
    }

    @Override
    public void close() {
        synchronized (lock) {
            closed = true;
            lock.notifyAll();
        }
    }
}
