package com.yovie;

import java.util.*;

public class LSMTree {
    private static final int THRESHOLD = 1000;
    private static final int COMPACTION_THRESHOLD = 1000;
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
}