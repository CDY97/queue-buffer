package com.cdy.queueBuffer.bean;

import java.util.Objects;

public class WriteBufferBean<K, V> {
    private String type;
    private K key;
    private V object;

    public WriteBufferBean(String type, K key, V object) {
        this.type = type;
        this.key = key;
        this.object = object;
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        WriteBufferBean<K, V> that = (WriteBufferBean<K, V>) o;
        return Objects.equals(key, that.key) && Objects.equals(type, that.type);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, type);
    }
}
