package com.cdy.queueBuffer.bean;

import java.util.Objects;

public class WriteBufferBean<K, V> {
    private String type;
    private K key;
    private V object;
    private long timeStamp;

    public WriteBufferBean(String type, K key, V object, long timeStamp) {
        this.type = type;
        this.key = key;
        this.object = object;
        this.timeStamp = timeStamp;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public K getKey() {
        return key;
    }

    public void setKey(K key) {
        this.key = key;
    }

    public V getObject() {
        return object;
    }

    public void setObject(V object) {
        this.object = object;
    }

    public long getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(long timeStamp) {
        this.timeStamp = timeStamp;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof WriteBufferBean)) return false;
        WriteBufferBean<?, ?> that = (WriteBufferBean<?, ?>) o;
        return timeStamp == that.timeStamp &&
                Objects.equals(type, that.type) &&
                Objects.equals(key, that.key);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, key, timeStamp);
    }
}
