package com.yovie;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class MemTable {
    private ConcurrentHashMap<String, String> table;
    public ConcurrentHashMap<String, Boolean> deletedKeys;

    public MemTable() {
        table = new ConcurrentHashMap<>();
        deletedKeys = new ConcurrentHashMap<>();
    }

    public void put(String key, String value) {
        table.put(key, value);
        // 当新插入或更新数据时，如果键之前被标记为删除，则移除删除标记
        deletedKeys.remove(key);
    }

    public String get(String key) {
        // 首先检查键是否被标记为删除
        if (deletedKeys.containsKey(key)) {
            return null;
        }
        // 如果没有被删除，则返回对应的值
        return table.get(key);
    }

    public void delete(String key) {
        // 将键标记为删除
        deletedKeys.put(key, true);
        // 从 table 中移除键值对
        table.remove(key);
    }

    public int size() {
        // 返回未被删除的键的数量
        return table.keySet().size() - deletedKeys.keySet().size();
    }

    public SSTable flushToDisk() {
        // 创建一个新的 SSTable 实例，只包含未被删除的键值对
        Map<String, String> dataToFlush = new ConcurrentHashMap<>();
        table.forEach((key, value) -> {
            if (!deletedKeys.containsKey(key)) {
                dataToFlush.put(key, value);
            }
        });
        return new SSTable(dataToFlush);
    }
}