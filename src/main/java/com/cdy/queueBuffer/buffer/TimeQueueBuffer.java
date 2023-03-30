package com.cdy.queueBuffer.buffer;

import com.cdy.queueBuffer.bean.LatestObjBean;
import com.cdy.queueBuffer.bean.TimeQueueBean;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class TimeQueueBuffer<K, V> {
    // 有效时间范围最大值1000s，根据队列总长度，不能超过1048575（2^20 - 1）计算而来
    private static final int MaxAliveTimeRange = 1000;
    // 为了解决数据时间戳略微超过右边界从而无法缓存的问题引入，拉长右边界
    private static final int FutureAliveTimeRange = 5;
    // 淘汰过期缓存范围
    private static final int ReleaseTimeRange = 5;
    // queueBeginTsAndIndex分割位
    private static final int SplitIndex = 20;
    // 队列下标在queueBeginTsAndIndex中的掩码
    private static final int QueueBeginIndexMask = (1 << SplitIndex) - 1;

    // 队列总时长
    private int allTimeRange = 70;
    // 有效时间范围
    private int aliveTimeRange = 60;
    // 队列总长度，不能超过1048575（2^20 - 1）
    private int queueSize = allTimeRange * 1000;
    // 是否缓存当前真实时间范围内的数据，如果定义了customBeginTs即为false
    private boolean isCurFlag = true;
    // 自定义开始缓存的时间戳
    private long customBeginTs = 0;

    // 时间窗口左边界对应时间戳和对应队列下标，前44位代表时间戳，后20位代表队列下标，放在一个变量中是为了保持两个变量的操作原子化且不加锁
    private volatile long queueBeginTsAndIndex = 0;
    private long lastRefreshTs = 0;

    private TimeQueueBean<K, V>[] queue = null;

    private Map<String, Map<K, LatestObjBean<V>>> latestMap = new ConcurrentHashMap<>();

    private AtomicInteger size = new AtomicInteger();

    public void setBeginTs(long beginTs) {
        if (beginTs <= 0) {
            throw new IllegalArgumentException("beginTs must be a timeStamp");
        }
        this.customBeginTs = beginTs;
        this.isCurFlag = false;
    }

    public void setAliveTimeRange(int timeRange) {
        if (timeRange <= 0 || timeRange > MaxAliveTimeRange) {
            throw new IllegalArgumentException("timeRange must be between 1 second and 1000 seconds");
        }
        this.aliveTimeRange = timeRange;
        this.allTimeRange = this.aliveTimeRange + 2 * FutureAliveTimeRange;
        this.queueSize = this.allTimeRange * 1000;
    }

    /**
     * 初始化环状数组开始时间、对应下标、上次刷新时间、缓存容量
     */
    public void initParams() {
        long queueBeginTs = 0, curTime = System.currentTimeMillis();
        int queueBeginIndex = 0;
        if (this.isCurFlag) {
            queueBeginTs = curTime - this.aliveTimeRange * 1000 + 1;
        } else {
            queueBeginTs = this.customBeginTs - this.aliveTimeRange * 1000 + 1;
        }
        this.queueBeginTsAndIndex = getQueueBeginTsAndIndex(queueBeginTs, queueBeginIndex);
        this.lastRefreshTs = curTime;
        this.queue = new TimeQueueBean[this.queueSize];
        this.size = new AtomicInteger();
        this.latestMap = new ConcurrentHashMap<>();
    }

    public int getSize() {
        return size.get();
    }

    public int getLatestOneSize() {
        int size = 0;
        for (Map.Entry<String, Map<K, LatestObjBean<V>>> entry : this.latestMap.entrySet()) {
            size += entry.getValue().size();
        }
        return size;
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
     * 通过queueBeginTsAndIndex获取可缓存范围开始时间
     * @param val
     * @return
     */
    private long getQueueBeginTs(long val) {
        return val >> SplitIndex;
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
     * 判断数据时间戳是否已超出可缓存范围
     * @param timeStamp
     * @return
     */
    public boolean checkTimeStamp(long timeStamp) {
        long queueBeginTs = getQueueBeginTs(this.queueBeginTsAndIndex);
        if (timeStamp < queueBeginTs || timeStamp - queueBeginTs >= (this.aliveTimeRange + FutureAliveTimeRange) * 1000) {
            return false;
        }
        return true;
    }

    /**
     * 缓冲同步至缓存时调用
     * @param type
     * @param key
     * @param object
     * @param timeStamp
     * @return
     */
    public void putObject(String type, K key, V object, long timeStamp) {
        int index = getQueueIndexByTimeStamp(timeStamp);
        TimeQueueBean queueObj = queue[index];
        // 双检锁方式创建TimeQueueBean
        if (queueObj == null) {
            synchronized (queue) {
                if (queue[index] == null) {
                    queueObj = new TimeQueueBean();
                    queue[index] = queueObj;
                }
            }
        }
        int incr = queueObj.putObject(type, key, object);
        size.addAndGet(incr);
    }

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
    public void flashAndRelease() {
        long tempVal = this.queueBeginTsAndIndex;
        // 以新下标开始向前删除5秒的缓存数据
        long queueBeginTs = getQueueBeginTs(tempVal);
        int queueBeginIndex = getQueueBeginIndex(tempVal);
        int length = ReleaseTimeRange * 1000;
        int beginIndex = (queueBeginIndex - length + this.queueSize) % this.queueSize;
        int endIndex = (queueBeginIndex - 1 + this.queueSize) % this.queueSize;
        if (beginIndex > endIndex) {
            for (int i = beginIndex; i < this.queueSize; i++) {
                TimeQueueBean<K, V> bean = queue[i];
                if (bean != null) {
                    size.getAndAdd(-bean.getSize());
                }
            }
            for (int i = 0; i <= endIndex; i++) {
                TimeQueueBean<K, V> bean = queue[i];
                if (bean != null) {
                    size.getAndAdd(-bean.getSize());
                }
            }
            Arrays.fill(queue, beginIndex, this.queueSize, null);
            Arrays.fill(queue, 0, endIndex + 1, null);
        } else {
            for (int i = beginIndex; i <= endIndex; i++) {
                TimeQueueBean<K, V> bean = queue[i];
                if (bean != null) {
                    size.getAndAdd(-bean.getSize());
                }
            }
            Arrays.fill(queue, beginIndex, endIndex + 1, null);
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
    }

    /**
     * 刷新环状数组可缓存范围的开始时间
     */
    public void refreshBeginTs() {
        long tempVal = this.queueBeginTsAndIndex;
        // 根据真实时间差推动时间轮的beginTs
        // 时间轮有效时间窗口左边界对应环状数组下标
        long curTs = System.currentTimeMillis();
        // 新的下标
        int queueBeginIndex = (int) (getQueueBeginIndex(tempVal) + curTs - lastRefreshTs) % this.queueSize;
        // 新的beginTs
        long queueBeginTs = getQueueBeginTs(tempVal) + curTs - lastRefreshTs;
        this.queueBeginTsAndIndex = getQueueBeginTsAndIndex(queueBeginTs, queueBeginIndex);
        this.lastRefreshTs = curTs;
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
     * 通过type和时间范围获取时间序列缓存
     * @param type
     * @param startTime
     * @param endTime
     * @return
     */
    public Map<Long, Map<K, V>> getAllByTypeAndTimeRange(String type, long startTime, long endTime, boolean isLatest) {
        Map<Long, Map<K, V>> resMap = new LinkedHashMap<>();
        if (type == null) {
            return resMap;
        }
        long tempVal = this.queueBeginTsAndIndex;
        int queueBeginIndex = getQueueBeginIndex(tempVal);
        long queueBeginTs = getQueueBeginTs(tempVal);
        // 如果取最新时间范围则计算环状数组最新范围下标，否则使用用户传的时间范围
        if (isLatest) {
            long intervel = endTime - startTime;
            endTime = queueBeginTs + this.aliveTimeRange * 1000 - 1;
            startTime = endTime - intervel;
        } else {
            startTime = Math.max(startTime, queueBeginTs);
            endTime = Math.min(endTime, queueBeginTs + (this.aliveTimeRange + FutureAliveTimeRange) * 1000 - 1);
        }
        if (startTime > endTime) {
            return resMap;
        }
        int beginIndex = getQueueIndexByTimeStamp(startTime);
        int endIndex = getQueueIndexByTimeStamp(endTime);
        if (beginIndex > endIndex) {
            // 环状数组起始时间与对应时间戳的差
            long tempInterval = queueBeginTs - queueBeginIndex;
            for (int i = beginIndex; i < this.queueSize; i++) {
                TimeQueueBean<K, V> bean = queue[i];
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
                TimeQueueBean<K, V> bean = queue[i];
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
                TimeQueueBean<K, V> bean = queue[i];
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
    public Map<Long, V> getAllByKeyAndTimeRange(String type, K key, long startTime, long endTime, boolean isLatest) {
        Map<Long, V> resMap = new LinkedHashMap<>();
        if (type == null || key == null) {
            return resMap;
        }
        long tempVal = this.queueBeginTsAndIndex;
        int queueBeginIndex = getQueueBeginIndex(tempVal);
        long queueBeginTs = getQueueBeginTs(tempVal);
        if (isLatest) {
            long intervel = endTime - startTime;
            endTime = queueBeginTs + this.aliveTimeRange * 1000 - 1;
            startTime = endTime - intervel;
        } else {
            startTime = Math.max(startTime, queueBeginTs);
            endTime = Math.min(endTime, queueBeginTs + (this.aliveTimeRange + FutureAliveTimeRange) * 1000 - 1);
        }
        if (startTime > endTime) {
            return resMap;
        }
        int beginIndex = getQueueIndexByTimeStamp(startTime);
        int endIndex = getQueueIndexByTimeStamp(endTime);
        if (beginIndex > endIndex) {
            long tempInterval = queueBeginTs - queueBeginIndex;
            for (int i = beginIndex; i < this.queueSize; i++) {
                TimeQueueBean<K, V> bean = queue[i];
                if (bean != null) {
                    V obj = bean.searchByKey(type, key);
                    if (obj != null) {
                        resMap.put(tempInterval + i, obj);
                    }
                }
            }
            tempInterval = queueBeginTs - queueBeginIndex + this.queueSize;
            for (int i = 0; i <= endIndex; i++) {
                TimeQueueBean<K, V> bean = queue[i];
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
                TimeQueueBean<K, V> bean = queue[i];
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

    public Map<K, V> getLatestOneByType(String type, long timeWindow) {
        Map<K, V> resMap = new HashMap<>();
        if (type == null) {
            return resMap;
        }
        long beginTs = getQueueBeginTs(this.queueBeginTsAndIndex) + this.aliveTimeRange * 1000 - timeWindow;
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

    public V getLatestOneByKey(String type, K key, long timeWindow) {
        V resObj = null;
        if (type == null || key == null) {
            return resObj;
        }
        long beginTs = getQueueBeginTs(this.queueBeginTsAndIndex) + this.aliveTimeRange * 1000 - timeWindow;
        Map<K, LatestObjBean<V>> typeMap = this.latestMap.get(type);
        if (typeMap != null) {
            LatestObjBean<V> bean = typeMap.get(key);
            if (bean != null && bean.getTimeStamp() >= beginTs) {
                resObj = bean.getObj();
            }
        }
        return resObj;
    }
}
