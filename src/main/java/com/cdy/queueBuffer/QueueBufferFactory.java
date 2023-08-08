package com.cdy.queueBuffer;

import com.cdy.queueBuffer.buffer.QueueBuffer;
import com.cdy.queueBuffer.buffer.QueueBufferImpl;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class QueueBufferFactory {

    private static Map<String, QueueBuffer> factoryOfQueueBuffer = new ConcurrentHashMap();

    /**
     * 线程安全地获得QueueBuffer
     * @param key
     * @return
     */
    public static <K, V> QueueBuffer<K, V> getInstance(String key) {
        return factoryOfQueueBuffer.computeIfAbsent(key, k -> new QueueBufferImpl(key));
    }

}
