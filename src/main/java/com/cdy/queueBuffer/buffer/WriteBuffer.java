package com.cdy.queueBuffer.buffer;

import com.cdy.queueBuffer.bean.WriteBufferBean;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class WriteBuffer<K, V> {

    private Map<WriteBufferBean<K, V>, Object>[] buffer;

    private volatile int taskIdsMapperInt = 0;

    private AtomicInteger size = new AtomicInteger();

    public Map<WriteBufferBean<K, V>, Object>[] getBuffer() {
        return buffer;
    }

    public List<Integer> getTaskIds() {
        List<Integer> taskIds = new ArrayList<>();
        for (int i = 0; i < buffer.length; i++) {
            if ((taskIdsMapperInt >> i & 1) == 1) {
                taskIds.add(i);
            }
        }
        return taskIds;
    }

    public WriteBuffer(int syncParallelism) {
        this.buffer = new Map[syncParallelism];
        for (int i = 0; i < syncParallelism; i++) {
            buffer[i] = new ConcurrentHashMap<>();
        }
    }

    public int getSize() {
        return size.get();
    }

    public void write(WriteBufferBean<K, V> vo) {
        Map<WriteBufferBean<K, V>, Object> map = buffer[(size.getAndIncrement() % buffer.length)];
        map.put(vo, true);
        // 设置时间戳对应的任务标识
        taskIdsMapperInt |= (1 << (int) (vo.getTimeStamp() % buffer.length));
    }
}
