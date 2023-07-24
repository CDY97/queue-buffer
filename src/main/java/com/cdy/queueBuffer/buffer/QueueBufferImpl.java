package com.cdy.queueBuffer.buffer;

import com.cdy.queueBuffer.bean.WriteBufferBean;
import executor.BufferExecutor;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.StampedLock;

public class QueueBufferImpl<K, V> implements QueueBuffer<K, V> {
    private int syncParallelism = 4;
    private int changeBufferInterval = 1;
    private int releaseInterval = 1000;
    private int retryCount = 3;

    private TimeQueueBuffer<K, V> timeQueueBuffer = new TimeQueueBuffer();
    private volatile WriteBuffer writeBuffer = new WriteBuffer(syncParallelism);
    private Set<K> queueTypeTable = new HashSet<>();

    private volatile Thread thread = null;

    private StampedLock putAndSyncLock = new StampedLock();

    private BufferExecutor executor = null;

    /**
     * 重新设置可缓存开始时间点
     * @param timeStamp
     * @return
     */
    @Override
    public QueueBuffer<K, V> changeStartTimeStamp(long timeStamp) {
        if (thread == null || thread != null && !thread.isAlive()) {
            timeQueueBuffer.setBeginTs(timeStamp);
        }
        return this;
    }

    /**
     * 设置缓存过期时间
     * @param seconds
     * @return
     */
    @Override
    public QueueBuffer<K, V> setAliveTimeSeconds(int seconds) {
        if (thread == null || thread != null && !thread.isAlive()) {
            timeQueueBuffer.setAliveTimeRange(seconds);
        }
        return this;
    }

    /**
     * 设置可缓存的时间戳大于当前的时间戳的最大秒数
     * @param seconds
     * @return
     */
    @Override
    public QueueBuffer<K, V> setFutureAliveTimeRangeSeconds(int seconds) {
        if (thread == null || thread != null && !thread.isAlive()) {
            timeQueueBuffer.setFutureAliveTimeRange(seconds);
        }
        return this;
    }

    /**
     * 设置缓冲Map数，值越大发生锁争抢概率越低，但更占内存
     * @param syncParallelism
     * @return
     */
    @Override
    public QueueBuffer<K, V> setSyncParallelism(int syncParallelism) {
        if (thread == null || thread != null && !thread.isAlive()) {
            if (changeBufferInterval <= 0) {
                throw new IllegalArgumentException("syncParallelism must be greater than 0");
            } else if (changeBufferInterval > 32) {
                throw new IllegalArgumentException("syncParallelism must be smaller than 33");
            }
            this.syncParallelism = syncParallelism;
        }
        return this;
    }

    /**
     * 设置切换缓冲对象毫秒数
     * @param changeBufferInterval
     * @return
     */
    @Override
    public QueueBuffer<K, V> setChangeBufferIntervalMs(int changeBufferInterval) {
        if (thread == null || thread != null && !thread.isAlive()) {
            if (changeBufferInterval <= 0) {
                throw new IllegalArgumentException("changeBufferInterval must be greater than 0 ms");
            }
            this.changeBufferInterval = changeBufferInterval;
        }
        return this;
    }

    /**
     * 设置执行清除过期缓存任务的间隔毫秒数
     * @param releaseInterval
     * @return
     */
    @Override
    public QueueBuffer<K, V> setReleaseIntervalMs(int releaseInterval) {
        if (thread == null || thread != null && !thread.isAlive()) {
            if (releaseInterval <= 0) {
                throw new IllegalArgumentException("releaseInterval must be greater than 0 ms");
            }
            this.releaseInterval = releaseInterval;
            this.timeQueueBuffer.setReleaseTimeRange((int) Math.ceil(1.0 * releaseInterval / 1000));
        }
        return this;
    }

    /**
     * 设置乐观写重试次数
     * @param retryCount
     * @return
     */
    @Override
    public QueueBuffer<K, V> setOptimisticPutRetryCount(int retryCount) {
        if (thread == null || thread != null && !thread.isAlive()) {
            this.retryCount = retryCount;
        }
        return this;
    }

    /**
     * 启动缓存
     */
    @Override
    public synchronized QueueBuffer<K, V> start() {
        if (thread == null) {
            timeQueueBuffer.initParams();
            executor = BufferExecutor.getInstance(syncParallelism);
            thread = new Thread(new SyncTask());
            thread.setPriority(Thread.MAX_PRIORITY);
            thread.start();
        } else {
            restart();
        }
        return this;
    }

    /**
     * 停止缓存
     */
    @Override
    public synchronized QueueBuffer<K, V> shutDown() {
        thread.interrupt();
        writeBuffer = new WriteBuffer(syncParallelism);
        timeQueueBuffer.initParams();
        executor.shutDown();
        return this;
    }

    /**
     * 重启缓存
     */
    @Override
    public synchronized QueueBuffer<K, V> restart() {
        if (thread != null && !thread.isAlive()) {
            writeBuffer = new WriteBuffer(syncParallelism);
            timeQueueBuffer.initParams();
            executor = BufferExecutor.getInstance(syncParallelism);
            thread = new Thread(new SyncTask());
            thread.setPriority(Thread.MAX_PRIORITY);
            thread.start();
        }
        return this;
    }

    /**
     * 设置缓存类型集合
     * @param types
     */
    @Override
    public QueueBuffer<K, V> setAllTypes(K... types) {
        if (queueTypeTable.isEmpty()) {
            for (K type : types) {
                queueTypeTable.add(type);
            }
        }
        return this;
    }

    @Override
    public int getSize() {
        return timeQueueBuffer.getSize();
    }

    @Override
    public int getLatestOneSize() {
        return timeQueueBuffer.getLatestOneSize();
    }

    /**
     * 写入缓冲
     * @param type
     * @param key
     * @param object
     * @param timeStamp
     */
    @Override
    public boolean put(String type, K key, V object, long timeStamp) {
        if (type == null || key == null || object == null) {
            throw new NullPointerException();
        }
        // 校验时间戳是否在可缓存范围内
        if (timeQueueBuffer.checkTimeStamp(timeStamp)) {
            WriteBufferBean<K, V> bean = new WriteBufferBean(type, key, object, timeStamp);
            // 尝试写入，通过缓冲的writable字段判断是否发生引用切换，若写入过程中发生了writeBuffer引用切换，则重试
            if (tryPut(bean)) {
                return true;
            } else {
                for (int i = 0; i < retryCount; i++) {
                    if (!timeQueueBuffer.checkTimeStamp(timeStamp)) {
                        return false;
                    }
                    if (tryPut(bean)) {
                        return true;
                    }
                }
                // 重试了RetryCount次后获取悲观读锁进行写入
                if (timeQueueBuffer.checkTimeStamp(timeStamp)) {
                    getReadLockAndPut(bean);
                    return true;
                } else {
                    return false;
                }
            }
        } else {
            return false;
        }
    }

    /**
     * 写入缓冲
     * @param bean
     * @return
     */
    public boolean tryPut(WriteBufferBean<K, V> bean) {
        return this.writeBuffer.write(bean);
    }

    /**
     * 获取悲观读锁写入缓冲
     * @param bean
     */
    public void getReadLockAndPut(WriteBufferBean<K, V> bean) {
        long stamp = putAndSyncLock.readLock();
        try {
            this.writeBuffer.write(bean);
        } finally {
            putAndSyncLock.unlockRead(stamp);
        }
    }

    /**
     * 通过type和时间范围获取全部缓存
     * @param type
     * @param startTime
     * @param endTime
     * @return
     */
    @Override
    public Map<Long, Map<K, V>> getAllByTypeAndTimeRange(String type, long startTime, long endTime) {
        return timeQueueBuffer.getAllByTypeAndTimeRange(type, startTime, endTime, false);
    }

    /**
     * 通过type、key和时间范围获取全部缓存
     * @param type
     * @param key
     * @param startTime
     * @param endTime
     * @return
     */
    @Override
    public Map<Long, V> getAllByKeyAndTimeRange(String type, K key, long startTime, long endTime) {
        return timeQueueBuffer.getAllByKeyAndTimeRange(type, key, startTime, endTime, false);
    }

    /**
     * 通过type和最近时间范围获取全部缓存
     * @param type
     * @param timeWindow
     * @return
     */
    @Override
    public Map<Long, Map<K, V>> getLatestAllByType(String type, long timeWindow) {
        long curTime = System.currentTimeMillis();
        return timeQueueBuffer.getAllByTypeAndTimeRange(type, curTime - timeWindow + 1, curTime, true);
    }

    /**
     * 通过type、key和最近时间范围获取全部缓存
     * @param type
     * @param key
     * @param timeWindow
     * @return
     */
    @Override
    public Map<Long, V> getLatestAllByKey(String type, K key, long timeWindow) {
        long curTime = System.currentTimeMillis();
        return timeQueueBuffer.getAllByKeyAndTimeRange(type, key, curTime - timeWindow + 1, curTime, true);
    }

    /**
     * 通过type获取每个key对应的最新一条缓存
     * @param type
     * @param timeWindow
     * @return
     */
    @Override
    public Map<K, V> getLatestOneByType(String type, long timeWindow) {
        return this.timeQueueBuffer.getLatestOneByType(type, timeWindow);
    }

    /**
     * 通过type、key获取最新一条缓存
     * @param type
     * @param key
     * @param timeWindow
     * @return
     */
    @Override
    public V getLatestOneByKey(String type, K key, long timeWindow) {
        return this.timeQueueBuffer.getLatestOneByKey(type, key, timeWindow);
    }

    /**
     * 同步任务
     */
    class SyncTask implements Runnable {

        @Override
        public void run() {
            long releaseTime = System.currentTimeMillis();
            while (!thread.isInterrupted()) {
                try {
                    // 切换缓冲对象并返回旧缓冲，需要加读写锁，否则会有极少量数据丢失
                    WriteBuffer<K, V> oldBuffer = lockAndChangeWriteBuffer();
                    // 无锁化同步缓冲到缓存
                    syncBuffer(oldBuffer);
                    // 无锁化时间窗口推进+过期缓存淘汰
                    releaseTime = stepTimeWindowAndRelease(releaseTime);
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    try {
                        Thread.sleep(changeBufferInterval);
                    } catch (InterruptedException e) {
                        break;
                    }
                }
            }
        }

        /**
         * 切换缓冲对象并返回旧缓冲，若缓冲暂无数据则不切换，并返回空
         * @return
         */
        private WriteBuffer<K, V> lockAndChangeWriteBuffer() {
            WriteBuffer<K, V> oldBuffer = writeBuffer;
            // 若缓冲中无数据，则不创建新缓冲，复用之前的缓冲对象
            if (oldBuffer.getSize() <= 0) {
                return null;
            }
            WriteBuffer tempNewWriteBuffer = new WriteBuffer(syncParallelism);
            long stamp = putAndSyncLock.writeLock();
            try {
                // 将旧缓冲writable标志位置为false，正在向该缓冲里写数据的线程需要重新向新缓冲里写数据
                oldBuffer.close();
                writeBuffer = tempNewWriteBuffer;
            } finally {
                putAndSyncLock.unlockWrite(stamp);
                return oldBuffer;
            }
        }

        /**
         * 同步缓冲到缓存
         * @param buffer
         */
        private void syncBuffer(WriteBuffer<K, V> buffer) {
            if (buffer != null) {
                List<Integer> taskIds = buffer.getTaskIds();
                // 异步执行多个同步缓冲到latestMap的任务
                syncBufferToLatestMapWithAsync(buffer, taskIds);
                // 同步等待多个同步缓冲到queueBuffer的任务，需要在最后执行
                syncBufferToQueueBufferWithSync(buffer, taskIds);
            }
        }

        /**
         * 异步执行多个同步缓冲到latestMap的任务
         * @param buffer
         */
        private void syncBufferToLatestMapWithAsync(WriteBuffer<K, V> buffer, List<Integer> taskIds) {
            // 异步操作同步缓冲到latestMap中
            for (Integer taskId : taskIds) {
                CompletableFuture.runAsync(() -> {
                    buffer.getBuffer().strongConsistencyForEach((bean) -> {
                        // 根据时间戳判断数据属于当前任务的才同步
                        if (bean.getTimeStamp() % syncParallelism == taskId) {
                            timeQueueBuffer.putObjectInLatestMap(bean.getType(), bean.getKey(),
                                    bean.getObject(), bean.getTimeStamp());
                        }
                    });
                }, executor.threadPool());
            }
        }

        /**
         * 同步等待多个同步缓冲到queueBuffer的任务
         * @param buffer
         */
        private void syncBufferToQueueBufferWithSync(WriteBuffer<K, V> buffer, List<Integer> taskIds) {
            CompletableFuture[] asyncArr = new CompletableFuture[taskIds.size()];
            // 并行同步数据，因为并行任务每个线程负责同步的时间对应的map都是相互独立的，所以不会有并发问题
            for (int i = 0; i < asyncArr.length; i++) {
                Integer taskId = taskIds.get(i);
                asyncArr[i] = CompletableFuture.runAsync(() -> {
                    buffer.getBuffer().strongConsistencyForEach((bean) -> {
                        // 根据时间戳判断数据属于当前任务的才同步
                        if (bean.getTimeStamp() % syncParallelism == taskId) {
                            timeQueueBuffer.putObject(bean.getType(), bean.getKey(),
                                    bean.getObject(), bean.getTimeStamp());
                        }
                    });
                }, executor.threadPool());
            }
            // 等待并行同步完成
            CompletableFuture.allOf(asyncArr).join();
        }

        /**
         * 时间窗口步进+淘汰过期缓存
         * @param releaseTime
         * @return
         */
        private long stepTimeWindowAndRelease(long releaseTime) {
            long curTime = System.currentTimeMillis();
            if (curTime - releaseTime >= releaseInterval) {
                timeQueueBuffer.refreshBeginTs();
                // 异步删除过期缓存
                CompletableFuture.runAsync(() -> timeQueueBuffer.flashAndRelease(), executor.threadPool());
                releaseTime = curTime;
            }
            return releaseTime;
        }
    }

}
