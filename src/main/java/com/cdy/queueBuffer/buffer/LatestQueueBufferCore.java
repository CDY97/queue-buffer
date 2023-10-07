package com.cdy.queueBuffer.buffer;

import com.cdy.queueBuffer.bean.LatestObjBean;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class LatestQueueBufferCore<K, V> implements QueueBufferCore<K, V> {
    // 有效时间范围最大值1000s
    private static final int MaxAliveTimeRange = 1000;

    // 有效时间范围
    private int aliveTimeRange = 60;
    // 是否缓存当前真实时间范围内的数据，如果定义了customBeginTs即为false
    private boolean isCurFlag = true;
    // 自定义开始缓存的时间戳
    private long customBeginTs = 0;

    private volatile long queueBeginTs = 0;
    private long lastRefreshTs = 0;

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

        long curTime = System.currentTimeMillis();
        if (this.isCurFlag) {
            this.queueBeginTs = curTime - this.aliveTimeRange * 1000 + 1;
        } else {
            this.queueBeginTs = this.customBeginTs - this.aliveTimeRange * 1000 + 1;
        }
        this.size = new AtomicInteger();
        this.latestMap = new ConcurrentHashMap<>();
        this.lastRefreshTs = curTime;
    }

    @Override
    public int getSize() {
        return this.size.get();
    }

    @Override
    public int getLatestOneSize() {
        return this.getSize();
    }

    /**
     * 判断数据时间戳是否已超出可缓存范围
     * @param timeStamp
     * @return
     */
    @Override
    public boolean checkTimeStamp(long timeStamp) {
        return timeStamp >= this.queueBeginTs;
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
        // java8的ConcurrentHashMap的computeIfAbsent即使key存在时也会加锁，性能较差
        Map<K, LatestObjBean<V>> typeMap = this.latestMap.get(type);
        if (typeMap == null) {
            typeMap = this.latestMap.computeIfAbsent(type, k -> new ConcurrentHashMap<>());
        }
        LatestObjBean<V> bean = typeMap.get(key);
        if (bean == null) {
            bean = typeMap.computeIfAbsent(key, k -> new LatestObjBean(timeStamp, object));
            this.size.incrementAndGet();
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

    @Override
    public void putObjectInLatestMap(String type, K key, V object, long timeStamp) {
        putObject(type, key, object, timeStamp);
    }

    /**
     * 释放资源
     */
    @Override
    public synchronized void flushAndRelease() {
        long queueBeginTs = this.queueBeginTs;
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
                    size.decrementAndGet();
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
    @Override
    public synchronized void refreshBeginTs() {
        long curTs = System.currentTimeMillis();
        // 新的beginTs
        this.queueBeginTs += curTs - this.lastRefreshTs;
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
        Map<K, LatestObjBean<V>> typeMap = this.latestMap.get(type);
        if (typeMap != null) {
            for (Map.Entry<K, LatestObjBean<V>> entry : typeMap.entrySet()) {
                LatestObjBean<V> bean = entry.getValue();
                if (bean != null && bean.getTimeStamp() >= startTime && bean.getTimeStamp() <= endTime) {
                    Map<K, V> tsMap = resMap.computeIfAbsent(bean.getTimeStamp(), k -> new HashMap<>());
                    tsMap.put(entry.getKey(), bean.getObj());
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
        Map<K, LatestObjBean<V>> typeMap = this.latestMap.get(type);
        if (typeMap != null) {
            LatestObjBean<V> bean = typeMap.get(key);
            if (bean != null && bean.getTimeStamp() >= startTime && bean.getTimeStamp() <= endTime) {
                resMap.put(bean.getTimeStamp(), bean.getObj());
            }
        }
        return resMap;
    }

    @Override
    public Map<K, V> getLatestOneByType(String type, long timeWindow) {
        long beginTs = this.queueBeginTs;
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
        long beginTs = this.queueBeginTs;
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
