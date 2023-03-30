package com.cdy.queueBuffer.buffer;

import com.cdy.queueBuffer.bean.WriteBufferBean;
import com.cdy.queueBuffer.bean.WriteBufferBeanList;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.StampedLock;

public class QueueBufferImpl<K, V> implements QueueBuffer<K, V> {

    private TimeQueueBuffer<K, V> timeQueueBuffer = new TimeQueueBuffer();
    private volatile WriteBuffer writeBuffer = new WriteBuffer();
    private Set<K> queueTypeTable = new HashSet<>();

    private volatile Thread thread = null;

    private StampedLock putAndSyncLock = new StampedLock();

    private static final int SyncParallelism = 4;
    private static final int ChangeBufferInterval = 1;
    private static final int ReleaseInterval = 1000;
    private static final int RetryCount = 3;

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
     * 启动缓存
     */
    @Override
    public synchronized QueueBuffer<K, V> start() {
        if (thread == null) {
            timeQueueBuffer.initParams();
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
        timeQueueBuffer.initParams();
        writeBuffer = new WriteBuffer();
        return this;
    }

    /**
     * 重启缓存
     */
    @Override
    public synchronized QueueBuffer<K, V> restart() {
        if (thread != null && !thread.isAlive()) {
            writeBuffer = new WriteBuffer();
            timeQueueBuffer.initParams();
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
        // 校验时间戳是否在可缓存范围内
        if (timeQueueBuffer.checkTimeStamp(timeStamp) && type != null && key != null) {
            WriteBufferBean<K, V> bean = new WriteBufferBean(type, key, object);
            // 获取乐观读锁尝试写入，若写入过程中发生了writeBuffer引用切换，则重试
            if (tryPut(bean, timeStamp)) {
                return true;
            } else {
                for (int i = 0; i < RetryCount; i++) {
                    if (!timeQueueBuffer.checkTimeStamp(timeStamp)) {
                        return false;
                    }
                    if (tryPut(bean, timeStamp)) {
                        return true;
                    }
                }
                // 重试了RetryCount次后获取悲观读锁进行写入
                if (timeQueueBuffer.checkTimeStamp(timeStamp)) {
                    getReadLockAndPut(bean, timeStamp);
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
     * 获取乐观读锁写入缓冲
     * @param bean
     * @param timeStamp
     * @return
     */
    public boolean tryPut(WriteBufferBean<K, V> bean, long timeStamp) {
        long stamp = putAndSyncLock.tryOptimisticRead();
        this.writeBuffer.write(bean, timeStamp);
        return putAndSyncLock.validate(stamp);
    }

    /**
     * 获取悲观读锁写入缓冲
     * @param bean
     * @param timeStamp
     */
    public void getReadLockAndPut(WriteBufferBean<K, V> bean, long timeStamp) {
        long stamp = putAndSyncLock.readLock();
        try {
            this.writeBuffer.write(bean, timeStamp);
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
                        Thread.sleep(ChangeBufferInterval);
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
            WriteBuffer<K, V> oldBuffer = null;
            // 若缓冲中无数据，则不创建新缓冲，复用之前的缓冲对象
            if (writeBuffer.getSize() <= 0) {
                return oldBuffer;
            }
            WriteBuffer tempNewWriteBuffer = new WriteBuffer();
            long stamp = putAndSyncLock.writeLock();
            try {
                oldBuffer = QueueBufferImpl.this.writeBuffer;
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
                // 将多个缓冲map的数据根据时间范围切分为SyncParallelism个部分，用于并行处理
                List<WriteBufferBeanList<K, V>>[] bufferList = splitBuffer(buffer);
                // 异步执行多个同步缓冲到latestMap的任务
                syncBufferToLatestMapWithAsync(bufferList);
                // 同步等待多个同步缓冲到queueBuffer的任务，需要在最后执行
                syncBufferToQueueBufferWithSync(bufferList);
            }
        }

        /**
         * 异步执行多个同步缓冲到latestMap的任务
         * @param bufferArr
         */
        private void syncBufferToLatestMapWithAsync(List<WriteBufferBeanList<K, V>>[] bufferArr) {
            // 异步操作同步缓冲到latestMap中
            for (int i = 0; i < bufferArr.length; i++) {
                List<WriteBufferBeanList<K, V>> beanListVoList = bufferArr[i];
                CompletableFuture.runAsync(() -> {
                    for (WriteBufferBeanList<K, V> beanListVo : beanListVoList) {
                        long timeStamp = beanListVo.getTimeStamp();
                        List<WriteBufferBean<K, V>> beanList = beanListVo.getVoList();
                        synchronized (beanList) {
                            for (WriteBufferBean<K, V> bean : beanList) {
                                timeQueueBuffer.putObjectInLatestMap(bean.getType(), bean.getKey(), bean.getObject(), timeStamp);
                            }
                        }
                    }
                });
            }
        }

        /**
         * 同步等待多个同步缓冲到queueBuffer的任务
         * @param bufferList
         */
        private void syncBufferToQueueBufferWithSync(List<WriteBufferBeanList<K, V>>[] bufferList) {
            CompletableFuture[] asyncArr = new CompletableFuture[bufferList.length];
            // 并行同步数据，因为并行任务每个线程负责同步的时间对应的map都是相互独立的，所以不会有并发问题
            for (int i = 0; i < bufferList.length; i++) {
                asyncArr[i] = syncDataAsyncTask(bufferList[i]);
            }
            // 等待并行同步完成
            CompletableFuture.allOf(asyncArr).join();
        }

        /**
         * 将缓冲切分为多个部分，用于并行处理
         * @param buffer
         * @return
         */
        private List<WriteBufferBeanList<K, V>>[] splitBuffer(WriteBuffer<K, V> buffer) {
            Map<Long, List<WriteBufferBean<K, V>>>[] oldBufferArr = buffer.getBuffer();
            List<WriteBufferBeanList<K, V>> allBufferList = new ArrayList<>();
            // 将缓冲中四个map的数据都放到一个list里
            for (int i = 0; i < oldBufferArr.length; i++) {
                Map<Long, List<WriteBufferBean<K, V>>> oldBuffer = oldBufferArr[i];
                for (Map.Entry<Long, List<WriteBufferBean<K, V>>> entry : oldBuffer.entrySet()) {
                    allBufferList.add(new WriteBufferBeanList(entry.getKey(), entry.getValue()));
                }
            }
            // 根据每个时间戳对应的缓存list大小做排序
            allBufferList.sort(Comparator.comparingInt(a -> -a.getVoList().size()));
            // 若缓存的时间戳种类不超过SyncParallelism，则创建更小的数组
            int length = Math.min(SyncParallelism, allBufferList.size());
            List<WriteBufferBeanList<K, V>>[] resArr = new List[length];
            for (int i = 0; i < resArr.length; i++) {
                resArr[i] = new ArrayList<>();
            }
            int allSize = buffer.getSize(), curIndex = 0, curSize = 0, splitFlag = allSize / length;
            // 将缓存较平均地分为4份
            for (int i = 0; i < allBufferList.size(); i++) {
                WriteBufferBeanList<K, V> curList = allBufferList.get(i);
                resArr[curIndex].add(curList);
                curSize += curList.getVoList().size();
                if (curSize >= splitFlag && curIndex < length - 1) {
                    curIndex++;
                    splitFlag += allSize / length;
                }
            }
            return resArr;
        }

        /**
         * 创建异步任务，并行同步数据，每个任务负责不同时间段的数据
         * @param bufferList
         * @return
         */
        private CompletableFuture<Void> syncDataAsyncTask(List<WriteBufferBeanList<K, V>> bufferList) {
            return CompletableFuture.runAsync(() -> {
                for (WriteBufferBeanList<K, V> list : bufferList) {
                    List<WriteBufferBean<K, V>> buffer = list.getVoList();
                    synchronized (buffer) {
                        for (WriteBufferBean<K, V> vo : buffer) {
                            timeQueueBuffer.putObject(vo.getType(), vo.getKey(), vo.getObject(), list.getTimeStamp());
                        }
                    }
                }
            });
        }

        /**
         * 时间窗口步进+淘汰过期缓存
         * @param releaseTime
         * @return
         */
        private long stepTimeWindowAndRelease(long releaseTime) {
            long curTime = System.currentTimeMillis();
            if (curTime - releaseTime >= ReleaseInterval) {
                timeQueueBuffer.refreshBeginTs();
                // 异步删除过期缓存
                CompletableFuture.runAsync(() -> timeQueueBuffer.flashAndRelease());
                releaseTime = curTime;
            }
            return releaseTime;
        }
    }

}
