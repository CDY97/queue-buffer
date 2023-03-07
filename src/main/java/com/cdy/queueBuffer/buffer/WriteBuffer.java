package com.cdy.queueBuffer.buffer;

import com.cdy.queueBuffer.bean.WriteBufferBean;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class WriteBuffer<K, V> {

    private Map<Long, List<WriteBufferBean<K, V>>>[] buffer;

    private static final int length = 4;

    private AtomicInteger size = new AtomicInteger();

    public Map<Long, List<WriteBufferBean<K, V>>>[] getBuffer() {
        return buffer;
    }

    public WriteBuffer() {
        this.buffer = new Map[length];
        for (int i = 0; i < length; i++) {
            buffer[i] = new ConcurrentHashMap<>();
        }
    }

    public int getSize() {
        return size.get();
    }

    public void write(WriteBufferBean<K, V> vo, long timeStamp) {
        Map<Long, List<WriteBufferBean<K, V>>> map = buffer[(int) (timeStamp % length)];
        // java8的ConcurrentHashMap的computeIfAbsent即使key存在时也会加锁，性能较差
        // 因此先用无锁get判断是否已经创建list
        List<WriteBufferBean<K, V>> timeList = map.get(timeStamp);
        if (timeList == null) {
            timeList = map.computeIfAbsent(timeStamp, k -> new ArrayList<>());
        }
        synchronized (timeList) {
            timeList.add(vo);
        }
        size.getAndIncrement();
    }
}
