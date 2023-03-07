package com.cdy.queueBuffer.bean;

import java.util.List;

public class WriteBufferBeanList<K, V> {
    private long timeStamp;
    private List<WriteBufferBean<K, V>> voList;

    public WriteBufferBeanList(long timeStamp, List<WriteBufferBean<K, V>> voList) {
        this.timeStamp = timeStamp;
        this.voList = voList;
    }

    public List<WriteBufferBean<K, V>> getVoList() {
        return voList;
    }

    public void setVoList(List<WriteBufferBean<K, V>> voList) {
        this.voList = voList;
    }

    public long getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(long timeStamp) {
        this.timeStamp = timeStamp;
    }
}
