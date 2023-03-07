package com.cdy.queueBuffer.bean;

public class LatestObjBean<V> {
    private volatile long timeStamp;
    private V obj;

    public LatestObjBean(long timeStamp, V obj) {
        this.timeStamp = timeStamp;
        this.obj = obj;
    }

    public long getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(long timeStamp) {
        this.timeStamp = timeStamp;
    }

    public V getObj() {
        return obj;
    }

    public void setObj(V obj) {
        this.obj = obj;
    }
}
