package com.yovie;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class Databasedb {
    private Map<String, String> data = new ConcurrentHashMap<>();

    public void create(String key, String value) {
        data.put(key, value);
    }

    public String read(String key) {
        return data.get(key);
    }

    public void update(String key, String value) {
        data.put(key, value);
    }

    public void delete(String key) {
        data.remove(key);
    }
}