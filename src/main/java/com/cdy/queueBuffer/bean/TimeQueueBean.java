package com.cdy.queueBuffer.bean;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

public class TimeQueueBean<K, V> {

    private Map<String, Map<K, V>> obj = new ConcurrentHashMap<>();

    public int putObject(String type, K key, V object){
        int res = 0;
        Map<K, V> typeMap = obj.get(type);
        if (typeMap == null) {
            typeMap = obj.computeIfAbsent(type, k -> new ConcurrentHashMap<>());
        }
        if (typeMap.put(key, object) == null) {
            res = 1;
        }
        return res;
    }

    public int getSize() {
        int size = 0;
        for (Map.Entry<String, Map<K, V>> entry : obj.entrySet()) {
            Map<K, V> value = entry.getValue();
            size += value.size();
        }
        return size;
    }

    public Map<K, V> searchByType(String type){
        return obj.get(type);
    }

    public V searchByKey(String type, K key){
        Map<K, V> tmp = obj.get(type);
        if (Objects.nonNull(tmp)) {
            return tmp.get(key);
        } else {
            return null;
        }
    }
}
