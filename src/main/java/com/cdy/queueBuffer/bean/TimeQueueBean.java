package com.cdy.queueBuffer.bean;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

public class TimeQueueBean<K, V> {

    private Map<String, Map<K, V>> obj = new ConcurrentHashMap<>();

    // todo，返回值测试用，待删除
    public int putObject(String type, K key, V object){
        int res = 0;
        Map<K, V> typeMap = obj.get(type);
        if (typeMap == null) {
            typeMap = obj.computeIfAbsent(type, k -> new ConcurrentHashMap<>());
        }
        // todo，待删除
        if (typeMap.put(key, object) == null) {
            res = 1;
        }
        return res;
    }

    // todo，测试用，待删除
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
