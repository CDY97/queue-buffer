package com.cdy.queueBuffer.buffer;

import com.cdy.queueBuffer.bean.WriteBufferBean;
import com.cdy.queueBuffer.concurrent.ConcurrentQuickList;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class WriteBuffer<K, V> {

    private ConcurrentQuickList<WriteBufferBean<K, V>> buffer;

    private AtomicInteger taskIdsMapperInt;

    private volatile boolean writable;

    private int syncParallelism;

    public ConcurrentQuickList<WriteBufferBean<K, V>> getBuffer() {
        return this.buffer;
    }

    public List<Integer> getTaskIds() {
        List<Integer> taskIds = new ArrayList<>();
        int mask = this.taskIdsMapperInt.get();
        for (int i = 0; i < this.syncParallelism; i++) {
            if ((mask >> i & 1) == 1) {
                taskIds.add(i);
            }
        }
        return taskIds;
    }

    public WriteBuffer(int syncParallelism) {
        this.buffer = new ConcurrentQuickList<>();
        this.taskIdsMapperInt = new AtomicInteger();
        this.writable = true;
        this.syncParallelism = syncParallelism;
    }

    public int getSize() {
        return this.buffer.size();
    }

    public boolean write(WriteBufferBean<K, V> vo) {
        // 设置时间戳对应的任务标识
        taskIdsMapperInt.getAndUpdate(value -> value | (1 << (int) (vo.getTimeStamp() % this.syncParallelism)));
        this.buffer.add(vo);
        return this.writable;
    }

    public void close() {
        this.writable = false;
    }
}
