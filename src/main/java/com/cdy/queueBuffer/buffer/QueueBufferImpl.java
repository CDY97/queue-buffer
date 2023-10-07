package com.cdy.queueBuffer.buffer;

import com.cdy.queueBuffer.bean.WriteBufferBean;
import com.cdy.queueBuffer.executor.BufferExecutor;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.StampedLock;

public class QueueBufferImpl<K, V> implements QueueBuffer<K, V> {

    // 缓冲同步到缓存使用的并行线程数
    private int syncParallelism = 4;
    // 切换缓冲对象的频率（毫秒）
    private int changeBufferInterval = 1;
    // 删除过期数据的频率（毫秒）
    private int releaseInterval = 1000;
    // 写缓冲重试次数
    private int retryCount = 3;
    // 是否只保存每个key的最新数据
    private boolean latestOnly = false;
    // 是否缓存当前真实时间范围内的数据，如果定义了customBeginTs即为false
    private boolean isCurFlag = true;
    // 自定义开始缓存的时间戳
    private long customBeginTs = 0;
    // 有效时间范围
    private int aliveTimeRange = 60;
    // 为了解决数据时间戳略微超过右边界从而无法缓存的问题引入，拉长右边界
    private int futureAliveTimeRange = 5;
    // 用于删除过期数据的范围，需要大于删除时间间隔
    private int releaseTimeRange = 1;

    private QueueBufferCore<K, V> timeQueueBuffer = null;
    private volatile WriteBuffer writeBuffer = new WriteBuffer(syncParallelism);
    private Set<K> queueTypeTable = new HashSet<>();

    private volatile Thread thread = null;

    private StampedLock putAndSyncLock = new StampedLock();

    private BufferExecutor executor = null;
    private String insName;

    public QueueBufferImpl(String insName) {
        this.insName = insName;
    }

    /**
     * 重新设置可缓存开始时间点
     * @param timeStamp
     * @return
     */
    @Override
    public QueueBuffer<K, V> changeStartTimeStamp(long timeStamp) {
        if (this.checkThread()) {
            this.customBeginTs = timeStamp;
            this.isCurFlag = false;
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
        if (this.checkThread()) {
            this.aliveTimeRange = seconds;
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
        if (this.checkThread()) {
            this.futureAliveTimeRange = seconds;
        }
        return this;
    }

    /**
     * 设置同步缓存线程数
     * @param syncParallelism
     * @return
     */
    @Override
    public QueueBuffer<K, V> setSyncParallelism(int syncParallelism) {
        if (this.checkThread()) {
            if (syncParallelism <= 0) {
                throw new IllegalArgumentException("syncParallelism must be greater than 0");
            } else if (syncParallelism > 32) {
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
        if (this.checkThread()) {
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
        if (this.checkThread()) {
            if (releaseInterval <= 0) {
                throw new IllegalArgumentException("releaseInterval must be greater than 0 ms");
            }
            this.releaseInterval = releaseInterval;
            this.releaseTimeRange = (int) Math.ceil(1.0 * releaseInterval / 1000);
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
        if (this.checkThread()) {
            this.retryCount = retryCount;
        }
        return this;
    }

    /**
     * 设置是否只保存最新值
     * @param latestOnly
     * @return
     */
    @Override
    public QueueBuffer<K, V> setLatestOnly(boolean latestOnly) {
        if (this.checkThread()) {
            this.latestOnly = latestOnly;
        }
        return this;
    }

    private boolean checkThread() {
        return this.thread == null || this.thread != null && !this.thread.isAlive();
    }

    /**
     * 初始化queueBufferCore
     */
    private void initQueueBufferCore() {
        if (this.latestOnly) {
            this.timeQueueBuffer = new LatestQueueBufferCore<>();
        } else {
            this.timeQueueBuffer = new TimeQueueBufferCore<>();
        }
        this.timeQueueBuffer.initParams(this.isCurFlag, this.customBeginTs, this.aliveTimeRange,
                this.futureAliveTimeRange, this.releaseTimeRange);
    }

    /**
     * 启动缓存
     */
    @Override
    public synchronized QueueBuffer<K, V> start() {
        if (this.thread == null) {
            this.initQueueBufferCore();
            this.executor = BufferExecutor.getInstance(this.insName, this.syncParallelism);
            this.thread = new Thread(new SyncTask(), String.format("queueBuffer-%s-main", this.insName));
            this.thread.setPriority(Thread.MAX_PRIORITY);
            this.thread.start();
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
        this.thread.interrupt();
        this.initQueueBufferCore();
        this.writeBuffer = new WriteBuffer(this.syncParallelism);
        this.executor.shutDown();
        return this;
    }

    /**
     * 重启缓存
     */
    @Override
    public synchronized QueueBuffer<K, V> restart() {
        if (this.thread != null && !this.thread.isAlive()) {
            this.initQueueBufferCore();
            this.writeBuffer = new WriteBuffer(this.syncParallelism);
            this.executor = BufferExecutor.getInstance(this.insName, this.syncParallelism);
            this.thread = new Thread(new SyncTask(), String.format("queueBuffer-%s-main", this.insName));
            this.thread.setPriority(Thread.MAX_PRIORITY);
            this.thread.start();
        }
        return this;
    }

    /**
     * 设置缓存类型集合
     * @param types
     */
    @Override
    public QueueBuffer<K, V> setAllTypes(K... types) {
        if (this.queueTypeTable.isEmpty()) {
            for (K type : types) {
                this.queueTypeTable.add(type);
            }
        }
        return this;
    }

    @Override
    public int getSize() {
        return this.timeQueueBuffer.getSize();
    }

    @Override
    public int getLatestOneSize() {
        return this.timeQueueBuffer.getLatestOneSize();
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
        if (Objects.isNull(type) || Objects.isNull(key) || Objects.isNull(object)) {
            throw new NullPointerException();
        }
        // 校验时间戳是否在可缓存范围内
        if (this.timeQueueBuffer.checkTimeStamp(timeStamp)) {
            WriteBufferBean<K, V> bean = new WriteBufferBean(type, key, object, timeStamp);
            // 尝试写入，通过缓冲的writable字段判断是否发生引用切换，若写入过程中发生了writeBuffer引用切换，则重试
            if (tryPut(bean)) {
                return true;
            } else {
                for (int i = 0; i < this.retryCount; i++) {
                    if (!this.timeQueueBuffer.checkTimeStamp(timeStamp)) {
                        return false;
                    }
                    if (tryPut(bean)) {
                        return true;
                    }
                }
                // 重试了retryCount次后获取悲观读锁进行写入
                if (this.timeQueueBuffer.checkTimeStamp(timeStamp)) {
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
        long stamp = this.putAndSyncLock.readLock();
        try {
            this.writeBuffer.write(bean);
        } finally {
            this.putAndSyncLock.unlockRead(stamp);
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
        return this.timeQueueBuffer.getAllByTypeAndTimeRange(type, startTime, endTime, false);
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
        return this.timeQueueBuffer.getAllByKeyAndTimeRange(type, key, startTime, endTime, false);
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
        return this.timeQueueBuffer.getAllByTypeAndTimeRange(type, curTime - timeWindow + 1, curTime, true);
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
        return this.timeQueueBuffer.getAllByKeyAndTimeRange(type, key, curTime - timeWindow + 1, curTime, true);
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
     * 获取当前所有的类型
     * @return
     */
    @Override
    public List<String> getAllTypes() {
        return this.timeQueueBuffer.getAllTypes();
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
                if (!latestOnly) {
                    // 同步等待多个同步缓冲到queueBuffer的任务，需要在最后执行
                    syncBufferToQueueBufferWithSync(buffer, taskIds);
                }
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
                CompletableFuture.runAsync(() -> timeQueueBuffer.flushAndRelease(), executor.threadPool());
                releaseTime = curTime;
            }
            return releaseTime;
        }
    }

}
