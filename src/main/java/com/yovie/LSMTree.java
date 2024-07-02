package com.yovie;

import java.util.*;

public class LSMTree {
    private static final int THRESHOLD = 1024;
    private static final int COMPACTION_THRESHOLD = 1024;
    private MemTable memTable;
    private List<SSTable> ssTables;

    public LSMTree() {
        this.memTable = new MemTable();
        this.ssTables = new ArrayList<>();
    }

    public void put(String key, String value) {
        memTable.put(key, value);
        if (memTable.size() > THRESHOLD) {
            flushMemTable();
        }
    }

    private void flushMemTable() {
        SSTable newSSTable = memTable.flushToDisk();
        ssTables.add(newSSTable);
        memTable = new MemTable();
        if (ssTables.size() > COMPACTION_THRESHOLD) {
            compactSSTables();
        }
    }

    private void compactSSTables() {
        // 实现 SSTable 合并逻辑
    }

    public String get(String key) {
        String value = memTable.get(key);
        if (value != null) return value;
        
        for (SSTable ssTable : ssTables) {
            value = ssTable.get(key);
            if (value != null) return value;
        }
        return null;
    }

    public void delete(String key) {
        // 从 MemTable 中删除键值对
        memTable.delete(key);

        // 将删除操作记录到 WAL 日志
        //walManager.logOperation("DELETE", key, null);

        // 由于 SSTable 是不可变的，这里可以添加一个标记删除的逻辑
        // 例如，可以创建一个新的 MemTable 项来标记这个键已被删除
        // 这个标记可以是一个特殊的值，例如使用一个删除标记字符串，例如 "DELETED"
        memTable.put(key, "DELETED");

        // 检查是否需要将 MemTable 刷新到磁盘
        if (memTable.size() > THRESHOLD) {
            flushMemTable();
        }
    }
}