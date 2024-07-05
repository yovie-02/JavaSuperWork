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
        List<SSTable> sstablesToCompact = new ArrayList<>(ssTables);

        // 创建一个Map来存储合并后的数据
        Map<String, String> mergedData = new TreeMap<>();

        // 读取每个SSTable并合并数据
        for (SSTable ssTable : sstablesToCompact) {
            ssTable.readFromDisk(); // 确保SSTable数据是最新的
            for (Map.Entry<String, String> entry : ssTable.data.entrySet()) {
                // 如果键已经被删除，则跳过
                if (memTable.deletedKeys.containsKey(entry.getKey())) {
                    continue;
                }
                // 将键值对添加到合并数据中，如果有重复的键，新的值会覆盖旧的值
                mergedData.put(entry.getKey(), entry.getValue());
            }
        }

        // 创建一个新的SSTable并写入合并后的数据
        SSTable newSSTable = new SSTable(mergedData);
        newSSTable.writeDataToDisk();

        // 将新的SSTable添加到列表，并删除旧的SSTable
        ssTables.clear();
        ssTables.add(newSSTable);
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

        // 检查是否需要将 MemTable 刷新到磁盘
        if (memTable.size() > THRESHOLD) {
            flushMemTable();
        }
    }
}