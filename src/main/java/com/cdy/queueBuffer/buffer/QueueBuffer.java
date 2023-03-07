package com.cdy.queueBuffer.buffer;

import java.util.Map;

public interface QueueBuffer<K, V> {

    public QueueBuffer<K, V> changeStartTimeStamp(long timeStamp);

    public QueueBuffer<K, V> setAliveTimeSeconds(int seconds);

    public QueueBuffer<K, V> start();

    public QueueBuffer<K, V> shutDown();

    public QueueBuffer<K, V> restart();

    @Deprecated
    public QueueBuffer<K, V> setAllTypes(K... types);

    public int getSize();

    public int getLatestOneSize();

    public boolean put(String type, K key, V object, long timeStamp);

    public Map<Long, Map<K, V>> getAllByTypeAndTimeRange(String type, long startTime, long endTime);

    public Map<Long, V> getAllByKeyAndTimeRange(String type, K key, long startTime, long endTime);

    public Map<Long, Map<K, V>> getLatestAllByType(String type, long timeWindow);

    public Map<Long, V> getLatestAllByKey(String type, K key, long timeWindow);

    public Map<K, V> getLatestOneByType(String type, long timeWindow);

    public V getLatestOneByKey(String type, K key, long timeWindow);
}
