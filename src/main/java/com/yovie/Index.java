package com.yovie;

import java.util.*;

public class Index {
    private Map<String, List<Long>> index = new HashMap<>();

    public void addToIndex(String key, long position) {
        index.computeIfAbsent(key, k -> new ArrayList<>()).add(position);
    }

    public List<Long> search(String key) {
        return index.getOrDefault(key, Collections.emptyList());
    }
}