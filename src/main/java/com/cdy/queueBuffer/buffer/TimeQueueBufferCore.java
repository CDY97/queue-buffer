package com.cdy.queueBuffer.buffer;

import com.cdy.queueBuffer.bean.LatestObjBean;
import com.cdy.queueBuffer.bean.TimeQueueBean;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class TimeQueueBufferCore<K, V> implements QueueBufferCore<K, V> {
    // queueBeginTsAndIndex分割位
    private static final int SplitIndex = 20;
    // 队列下标在queueBeginTsAndIndex中的掩码
    private static final int QueueBeginIndexMask = (1 << SplitIndex) - 1;
    // 有效时间范围最大值1000s，根据队列总长度，不能超过1048575（2^20 - 1）计算而来
    private static final int MaxAliveTimeRange = 1000;

    // 有效时间范围
    private int aliveTimeRange = 60;
    // 为了解决数据时间戳略微超过右边界从而无法缓存的问题引入，拉长右边界
    private int futureAliveTimeRange = 5;
    // 用于删除过期数据的范围，需要大于删除时间间隔
    private int releaseTimeRange = 1;
    // 队列总时长
    private int allTimeRange = aliveTimeRange + futureAliveTimeRange + releaseTimeRange;
    // 队列总长度，不能超过1048575（2^20 - 1）
    private int queueSize = allTimeRange * 1000;
    // 是否缓存当前真实时间范围内的数据，如果定义了customBeginTs即为false
    private boolean isCurFlag = true;
    // 自定义开始缓存的时间戳
    private long customBeginTs = 0;

    // 时间窗口左边界对应时间戳和对应队列下标，前44位代表时间戳，后20位代表队列下标，放在一个变量中是为了保持两个变量的操作原子化且不加锁
    private volatile long queueBeginTsAndIndex = 0;
    private long lastRefreshTs = 0;
    private int lastReleaseIndex = queueSize - 1;

    private TimeQueueBean<K, V>[] queue = null;

    private Map<String, Map<K, LatestObjBean<V>>> latestMap = new ConcurrentHashMap<>();

    private AtomicInteger size = new AtomicInteger();

    /**
     * 初始化环状数组开始时间、对应下标、上次刷新时间、缓存容量
     */
    @Override
    public void initParams(boolean isCurFlag, long customBeginTs, int aliveTimeRange, int futureAliveTimeRange, int releaseTimeRange) {
        if (!isCurFlag && customBeginTs <= 0) {
            throw new IllegalArgumentException("startTimeStamp must be a timeStamp");
        }
        this.isCurFlag = isCurFlag;
        this.customBeginTs = customBeginTs;

        if (aliveTimeRange <= 0 || aliveTimeRange > MaxAliveTimeRange) {
            throw new IllegalArgumentException("aliveTime must be between 1 second and 1000 seconds");
        }
        this.aliveTimeRange = aliveTimeRange;

        if (futureAliveTimeRange <= 0) {
            throw new IllegalArgumentException("futureAliveTimeRange must be greater than 0 second");
        }
        this.futureAliveTimeRange = futureAliveTimeRange;

        if (releaseTimeRange <= 0) {
            throw new IllegalArgumentException("releaseTimeRange must be greater than 0 s");
        }
        this.releaseTimeRange = releaseTimeRange;

        long queueBeginTs = 0, curTime = System.currentTimeMillis();
        int queueBeginIndex = 0;
        if (this.isCurFlag) {
            queueBeginTs = curTime - this.aliveTimeRange * 1000 + 1;
        } else {
            queueBeginTs = this.customBeginTs - this.aliveTimeRange * 1000 + 1;
        }
        this.queueBeginTsAndIndex = getQueueBeginTsAndIndex(queueBeginTs, queueBeginIndex);
        this.allTimeRange = this.aliveTimeRange + this.futureAliveTimeRange + this.releaseTimeRange;
        if (this.allTimeRange > MaxAliveTimeRange) {
            throw new IllegalArgumentException("queue buffer is too large, aliveTime + futureAliveTimeRange" +
                    " + changeBufferInterval cannot be greater than 1000 seconds");
        }
        this.queueSize = this.allTimeRange * 1000;
        this.queue = new TimeQueueBean[this.queueSize];
        this.size = new AtomicInteger();
        this.latestMap = new ConcurrentHashMap<>();
        this.lastRefreshTs = curTime;
        this.lastReleaseIndex = queueSize - 1;
    }

    @Override
    public int getSize() {
        return this.size.get();
    }

    @Override
    public int getLatestOneSize() {
        int size = 0;
        for (Map.Entry<String, Map<K, LatestObjBean<V>>> entry : this.latestMap.entrySet()) {
            size += entry.getValue().size();
        }
        return size;
    }

    /**
     * 判断数据时间戳是否已超出可缓存范围
     * @param timeStamp
     * @return
     */
    @Override
    public boolean checkTimeStamp(long timeStamp) {
        long queueBeginTs = getQueueBeginTs(this.queueBeginTsAndIndex);
        if (timeStamp < queueBeginTs || timeStamp - queueBeginTs >= (this.aliveTimeRange + this.futureAliveTimeRange) * 1000) {
            return false;
        }
        return true;
    }

    /**
     * 通过queueBeginTsAndIndex获取队列起始位置下标
     * @param val
     * @return
     */
    private int getQueueBeginIndex(long val) {
        return (int) (val & QueueBeginIndexMask);
    }

    /**
     * 直接获取队列起始位置下标
     * @return
     */
    private int getQueueBeginIndex() {
        return (int) (this.queueBeginTsAndIndex & QueueBeginIndexMask);
    }

    /**
     * 通过queueBeginTsAndIndex获取可缓存范围开始时间
     * @param val
     * @return
     */
    private long getQueueBeginTs(long val) {
        return val >> SplitIndex;
    }

    /**
     * 直接获取时间窗口左边界时间戳
     * @return
     */
    private long getQueueBeginTs() {
        return this.queueBeginTsAndIndex >> SplitIndex;
    }

    /**
     * 通过队列起始位置下标和可缓存范围开始时间获取queueBeginTsAndIndex
     * @param queueBeginTs
     * @param queueBeginIndex
     * @return
     */
    private long getQueueBeginTsAndIndex(long queueBeginTs, int queueBeginIndex) {
        return queueBeginTs << SplitIndex | queueBeginIndex;
    }

    /**
     * 通过时间戳获取对应的环状数组下标
     * @param timeStamp
     * @return
     */
    private int getQueueIndexByTimeStamp(long timeStamp) {
        long tempVal = this.queueBeginTsAndIndex;
        int queueBeginIndex = getQueueBeginIndex(tempVal);
        long queueBeginTs = getQueueBeginTs(tempVal);
        return (int) (queueBeginIndex + (timeStamp - queueBeginTs)) % this.queueSize;
    }

    /**
     * 缓冲同步至缓存时调用，需要保证同一个时间戳的所有数据只被一个线程同步
     * @param type
     * @param key
     * @param object
     * @param timeStamp
     * @return
     */
    @Override
    public void putObject(String type, K key, V object, long timeStamp) {
        int index = getQueueIndexByTimeStamp(timeStamp);
        TimeQueueBean queueObj = this.queue[index];
        if (queueObj == null) {
            queueObj = new TimeQueueBean();
            this.queue[index] = queueObj;
        }
        int incr = queueObj.putObject(type, key, object);
        this.size.addAndGet(incr);
    }

    @Override
    public void putObjectInLatestMap(String type, K key, V object, long timeStamp) {
        // java8的ConcurrentHashMap的computeIfAbsent即使key存在时也会加锁，性能较差
        Map<K, LatestObjBean<V>> typeMap = this.latestMap.get(type);
        if (typeMap == null) {
            typeMap = this.latestMap.computeIfAbsent(type, k -> new ConcurrentHashMap<>());
        }
        LatestObjBean<V> bean = typeMap.get(key);
        if (bean == null) {
            bean = typeMap.computeIfAbsent(key, k -> new LatestObjBean(timeStamp, object));
        }
        // 如果旧缓存的时间戳比当前小，替换缓存
        if (bean.getTimeStamp() < timeStamp) {
            // 相当于对key加锁，比ConcurrentHashMap锁粒度更小
            synchronized (bean) {
                if (bean.getTimeStamp() < timeStamp) {
                    // 复用LatestObjBean
                    bean.setTimeStamp(timeStamp);
                    bean.setObj(object);
                }
            }
        }
    }

    /**
     * 释放资源
     */
    @Override
    public synchronized void flushAndRelease() {
        long tempVal = this.queueBeginTsAndIndex;
        // 以新下标开始向前删除releaseTimeRange秒的缓存数据
        long queueBeginTs = getQueueBeginTs(tempVal);
        int queueBeginIndex = getQueueBeginIndex(tempVal);
        int beginIndex = (this.lastReleaseIndex + 1) % this.queueSize;
        int endIndex = (queueBeginIndex - 1 + this.queueSize) % this.queueSize;
        if (beginIndex > endIndex) {
            for (int i = beginIndex; i < this.queueSize; i++) {
                TimeQueueBean<K, V> bean = this.queue[i];
                if (bean != null) {
                    this.size.getAndAdd(-bean.getSize());
                }
            }
            for (int i = 0; i <= endIndex; i++) {
                TimeQueueBean<K, V> bean = this.queue[i];
                if (bean != null) {
                    this.size.getAndAdd(-bean.getSize());
                }
            }
            Arrays.fill(this.queue, beginIndex, this.queueSize, null);
            Arrays.fill(this.queue, 0, endIndex + 1, null);
        } else {
            for (int i = beginIndex; i <= endIndex; i++) {
                TimeQueueBean<K, V> bean = this.queue[i];
                if (bean != null) {
                    this.size.getAndAdd(-bean.getSize());
                }
            }
            Arrays.fill(this.queue, beginIndex, endIndex + 1, null);
        }
        // 清除latestMap里的过期数据
        Iterator<Map.Entry<String, Map<K, LatestObjBean<V>>>> typeIterator = this.latestMap.entrySet().iterator();
        while (typeIterator.hasNext()) {
            Map.Entry<String, Map<K, LatestObjBean<V>>> typeEntry = typeIterator.next();
            Map<K, LatestObjBean<V>> objMap = typeEntry.getValue();
            Iterator<Map.Entry<K, LatestObjBean<V>>> objIterator = objMap.entrySet().iterator();
            while (objIterator.hasNext()) {
                Map.Entry<K, LatestObjBean<V>> objEntry = objIterator.next();
                LatestObjBean<V> objBean = objEntry.getValue();
                if (objBean != null && objBean.getTimeStamp() < queueBeginTs) {
                    objIterator.remove();
                }
            }
            if (objMap.size() == 0) {
                typeIterator.remove();
            }
        }
        this.lastReleaseIndex = endIndex;
    }

    /**
     * 刷新环状数组可缓存范围的开始时间
     */
    @Override
    public synchronized void refreshBeginTs() {
        long tempVal = this.queueBeginTsAndIndex;
        // 根据真实时间差推动时间轮的beginTs
        // 时间轮有效时间窗口左边界对应环状数组下标
        long curTs = System.currentTimeMillis();
        // 新的下标
        int queueBeginIndex = (int) (getQueueBeginIndex(tempVal) + curTs - this.lastRefreshTs) % this.queueSize;
        // 新的beginTs
        long queueBeginTs = getQueueBeginTs(tempVal) + curTs - this.lastRefreshTs;
        this.queueBeginTsAndIndex = getQueueBeginTsAndIndex(queueBeginTs, queueBeginIndex);
        this.lastRefreshTs = curTs;
    }

    /**
     * 通过type和时间范围获取时间序列缓存
     * @param type
     * @param startTime
     * @param endTime
     * @return
     */
    @Override
    public Map<Long, Map<K, V>> getAllByTypeAndTimeRange(String type, long startTime, long endTime, boolean isLatest) {
        Map<Long, Map<K, V>> resMap = new LinkedHashMap<>();
        long tempVal = this.queueBeginTsAndIndex;
        int queueBeginIndex = getQueueBeginIndex(tempVal);
        long queueBeginTs = getQueueBeginTs(tempVal);
        // 如果取最新时间范围则计算环状数组最新范围下标，否则使用用户传的时间范围
        startTime = Math.max(startTime, queueBeginTs);
        if (isLatest) {
            endTime = queueBeginTs + this.aliveTimeRange * 1000 - 1;
        } else {
            endTime = Math.min(endTime, queueBeginTs + (this.aliveTimeRange + this.futureAliveTimeRange) * 1000 - 1);
        }
        if (startTime > endTime) {
            return resMap;
        }
        int beginIndex = getQueueIndexByTimeStamp(startTime);
        int endIndex = getQueueIndexByTimeStamp(endTime);
        if (beginIndex < 0 || beginIndex >= this.queueSize || endIndex < 0 || endIndex >= this.queueSize) {
            return resMap;
        }
        if (beginIndex > endIndex) {
            // 环状数组起始时间与对应时间戳的差
            long tempInterval = queueBeginTs - queueBeginIndex;
            for (int i = beginIndex; i < this.queueSize; i++) {
                TimeQueueBean<K, V> bean = this.queue[i];
                if (bean != null) {
                    Map<K, V> typeMap = bean.searchByType(type);
                    if (typeMap != null && !typeMap.isEmpty()) {
                        resMap.put(tempInterval + i, typeMap);
                    }
                }
            }
            // 环状数组下标移动到数组首部后，下标从0开始计数，而此时对应时间戳应该还是递增的，因此差值加上数组长度
            tempInterval = queueBeginTs - queueBeginIndex + this.queueSize;
            for (int i = 0; i <= endIndex; i++) {
                TimeQueueBean<K, V> bean = this.queue[i];
                if (bean != null) {
                    Map<K, V> typeMap = bean.searchByType(type);
                    if (typeMap != null && !typeMap.isEmpty()) {
                        resMap.put(tempInterval + i, typeMap);
                    }
                }
            }
        } else {
            // 查询范围开始下标如果小于环状数组起始下标，则说明查询范围已从0开始计数，因此差值加上数组长度
            long tempInterval = beginIndex >= queueBeginIndex ? queueBeginTs - queueBeginIndex : queueBeginTs - queueBeginIndex + this.queueSize;
            for (int i = beginIndex; i <= endIndex; i++) {
                TimeQueueBean<K, V> bean = this.queue[i];
                if (bean != null) {
                    Map<K, V> typeMap = bean.searchByType(type);
                    if (typeMap != null && !typeMap.isEmpty()) {
                        resMap.put(tempInterval + i, typeMap);
                    }
                }
            }
        }
        return resMap;
    }

    /**
     * 通过type、key和时间范围获取时间序列缓存
     * @param type
     * @param key
     * @param startTime
     * @param endTime
     * @return
     */
    @Override
    public Map<Long, V> getAllByKeyAndTimeRange(String type, K key, long startTime, long endTime, boolean isLatest) {
        Map<Long, V> resMap = new LinkedHashMap<>();
        long tempVal = this.queueBeginTsAndIndex;
        int queueBeginIndex = getQueueBeginIndex(tempVal);
        long queueBeginTs = getQueueBeginTs(tempVal);
        startTime = Math.max(startTime, queueBeginTs);
        if (isLatest) {
            endTime = queueBeginTs + this.aliveTimeRange * 1000 - 1;
        } else {
            endTime = Math.min(endTime, queueBeginTs + (this.aliveTimeRange + this.futureAliveTimeRange) * 1000 - 1);
        }
        if (startTime > endTime) {
            return resMap;
        }
        int beginIndex = getQueueIndexByTimeStamp(startTime);
        int endIndex = getQueueIndexByTimeStamp(endTime);
        if (beginIndex < 0 || beginIndex >= this.queueSize || endIndex < 0 || endIndex >= this.queueSize) {
            return resMap;
        }
        if (beginIndex > endIndex) {
            long tempInterval = queueBeginTs - queueBeginIndex;
            for (int i = beginIndex; i < this.queueSize; i++) {
                TimeQueueBean<K, V> bean = this.queue[i];
                if (bean != null) {
                    V obj = bean.searchByKey(type, key);
                    if (obj != null) {
                        resMap.put(tempInterval + i, obj);
                    }
                }
            }
            tempInterval = queueBeginTs - queueBeginIndex + this.queueSize;
            for (int i = 0; i <= endIndex; i++) {
                TimeQueueBean<K, V> bean = this.queue[i];
                if (bean != null) {
                    V obj = bean.searchByKey(type, key);
                    if (obj != null) {
                        resMap.put(tempInterval + i, obj);
                    }
                }
            }
        } else {
            long tempInterval = beginIndex >= queueBeginIndex ? queueBeginTs - queueBeginIndex : queueBeginTs - queueBeginIndex + this.queueSize;
            for (int i = beginIndex; i <= endIndex; i++) {
                TimeQueueBean<K, V> bean = this.queue[i];
                if (bean != null) {
                    V obj = bean.searchByKey(type, key);
                    if (obj != null) {
                        resMap.put(tempInterval + i, obj);
                    }
                }
            }
        }
        return resMap;
    }

    @Override
    public Map<K, V> getLatestOneByType(String type, long timeWindow) {
        long beginTs = getQueueBeginTs(this.queueBeginTsAndIndex) + this.aliveTimeRange * 1000 - timeWindow;
        Map<K, V> resMap = new HashMap<>();
        Map<K, LatestObjBean<V>> typeMap = this.latestMap.get(type);
        if (typeMap != null) {
            for (Map.Entry<K, LatestObjBean<V>> entry : typeMap.entrySet()) {
                LatestObjBean<V> bean = entry.getValue();
                if (bean != null && bean.getTimeStamp() >= beginTs) {
                    resMap.put(entry.getKey(), bean.getObj());
                }
            }
        }
        return resMap;
    }

    @Override
    public V getLatestOneByKey(String type, K key, long timeWindow) {
        long beginTs = getQueueBeginTs(this.queueBeginTsAndIndex) + this.aliveTimeRange * 1000 - timeWindow;
        V resObj = null;
        Map<K, LatestObjBean<V>> typeMap = this.latestMap.get(type);
        if (typeMap != null) {
            LatestObjBean<V> bean = typeMap.get(key);
            if (bean != null && bean.getTimeStamp() >= beginTs) {
                resObj = bean.getObj();
            }
        }
        return resObj;
    }

    /**
     * 获取当前所有的类型
     * @return
     */
    @Override
    public List<String> getAllTypes() {
        return this.latestMap.keySet().stream().collect(Collectors.toList());
    }
}
