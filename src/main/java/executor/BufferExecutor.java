package executor;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class BufferExecutor {

    private Executor executor;

    public Executor threadPool() {
        return executor;
    }

    private BufferExecutor() {
    }

    private BufferExecutor(Executor executor) {
        this.executor = executor;
    }

    public static BufferExecutor getInstance(int coreSize) {
        Executor executor = new ThreadPoolExecutor(
                coreSize,
                coreSize * 2,
                1,
                TimeUnit.SECONDS,
                new SynchronousQueue(),
                new ThreadFactory() {

                    private AtomicInteger threadNumber = new AtomicInteger();
                    @Override
                    public Thread newThread(Runnable r) {
                        Thread t = new Thread(r, "queueBuffer-executor-thread-" + threadNumber.getAndIncrement());
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
}
