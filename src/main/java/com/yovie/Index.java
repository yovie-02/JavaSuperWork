package com.yovie;

import java.io.IOException;
import java.util.*;

public class Index {
    private Map<String, List<Long>> index = new HashMap<>();
//    private LSMTree lsmTree;
    private WALManager walManager;
//    private PersistenceManager persistenceManager;
//
    public Index() throws IOException {
//        lsmTree = new LSMTree();
        walManager = new WALManager();
//        persistenceManager = new PersistenceManager();
    }

    public void addToIndex(String key, long position) throws DatabaseException {
        try {
            index.computeIfAbsent(key, k -> new ArrayList<>()).add(position);
            walManager.logIndexOperation("INDEX_ADD", key, position); // 记录索引添加操作
        } catch (IOException e) {
            throw new DatabaseException("Failed to add index entry: " + e.getMessage());
        }
    }


    public List<Long> search(String key) throws DatabaseException {
        try {
            List<Long> positions = index.getOrDefault(key, Collections.emptyList());
            walManager.logSearchOperation("INDEX_SEARCH", key);
            return positions;
        }catch (IOException e){
            throw new DatabaseException("Failed to search index entry: " + e.getMessage());
        }
    }
}