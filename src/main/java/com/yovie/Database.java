package com.yovie;

import java.io.IOException;

public class Database {
    private LSMTree lsmTree;
    private WALManager walManager;
    private PersistenceManager persistenceManager;

    public void create(String key, String value) throws DatabaseException {
        try {
            walManager.logOperation("CREATE", key, value);
            lsmTree.put(key, value);
            persistenceManager.write(key, value);
        } catch (IOException e) {
            throw new DatabaseException("Failed to create: " + e.getMessage());
        }
    }

    public String read(String key) throws DatabaseException {
        try {
            return lsmTree.get(key);
        } catch (Exception e) {
            throw new DatabaseException("Failed to read: " + e.getMessage());
        }
    }

    // 实现 update 和 delete 方法，类似于 create 方法

    public void backup() throws DatabaseException {
        // 实现备份逻辑
    }

    public void restore(String backupFile) throws DatabaseException {
        // 实现恢复逻辑
    }
}
