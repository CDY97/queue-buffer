package com.cdy.queueBuffer.buffer;

import java.util.List;
import java.util.Map;

public interface QueueBufferCore<K, V> {
    void initParams(boolean isCurFlag, long customBeginTs, int aliveTimeRange, int futureAliveTimeRange, int releaseTimeRange);

    int getSize();

    int getLatestOneSize();

    boolean checkTimeStamp(long timeStamp);

    Map<Long, Map<K,V>> getAllByTypeAndTimeRange(String type, long startTime, long endTime, boolean isLatest);

    Map<Long,V> getAllByKeyAndTimeRange(String type, K key, long startTime, long endTime, boolean isLatest);

    Map<K,V> getLatestOneByType(String type, long timeWindow);

    V getLatestOneByKey(String type, K key, long timeWindow);

    List<String> getAllTypes();

    void putObjectInLatestMap(String type, K key, V object, long timeStamp);

    void putObject(String type, K key, V object, long timeStamp);

    void refreshBeginTs();

    void flushAndRelease();
}
