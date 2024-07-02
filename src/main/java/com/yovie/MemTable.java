package com.yovie;

import java.util.concurrent.ConcurrentHashMap;

public class MemTable {
    private ConcurrentHashMap<String, String> table;

    public MemTable() {
        table = new ConcurrentHashMap<>();
    }

    public void put(String key, String value) {
        table.put(key, value);
    }

    public String get(String key) {
        return table.get(key);
    }

    public SSTable flushToDisk() {
        // 将内存中的数据持久化到SSTable
        return new SSTable(table);
    }

    public int size() {
        return table.size();
    }
}