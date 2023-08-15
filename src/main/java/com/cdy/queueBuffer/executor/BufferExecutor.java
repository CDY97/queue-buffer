package com.cdy.queueBuffer.executor;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class BufferExecutor {

    private ExecutorService executor;

    public ExecutorService threadPool() {
        return executor;
    }

    private BufferExecutor() {
    }

    private BufferExecutor(ExecutorService executor) {
        this.executor = executor;
    }

    public static BufferExecutor getInstance(String insName, int coreSize) {
        ExecutorService executor = new ThreadPoolExecutor(
                coreSize,
                coreSize * 2,
                1,
                TimeUnit.SECONDS,
                new SynchronousQueue(),
                new ThreadFactory() {

                    private AtomicInteger threadNumber = new AtomicInteger();
                    @Override
                    public Thread newThread(Runnable r) {
                        Thread t = new Thread(r, String.format("queueBuffer-%s-com.cdy.queueBuffer.executor-%s", insName,
                                threadNumber.getAndIncrement()));
                        if (t.isDaemon())
                            t.setDaemon(false);
                        if (t.getPriority() != Thread.NORM_PRIORITY)
                            t.setPriority(Thread.NORM_PRIORITY);
                        return t;
                    }
                },
                new ThreadPoolExecutor.CallerRunsPolicy()
        );
        return new BufferExecutor(executor);
    }

    public void shutDown() {
        executor.shutdown();
    }
}
