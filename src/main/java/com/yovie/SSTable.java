package com.yovie;

import java.io.*;
import java.util.*;

public class SSTable {
    private Map<String, String> data;
    private String filePath;

    public SSTable(Map<String, String> data) {
        this.data = data;
        this.filePath = "sstable_" + System.currentTimeMillis() + ".db";
        writeDataToDisk();
    }

    private void writeDataToDisk() {
        try (ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(filePath))) {
            out.writeObject(data);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public String get(String key) {
        return data.get(key);
    }

    // 这个方法可以根据实际的SSTable实现来调整，例如，如果SSTable是顺序存储的，则可能需要顺序读取文件
    public void readFromDisk() {
        try (ObjectInputStream in = new ObjectInputStream(new FileInputStream(filePath))) {
            data = (Map<String, String>) in.readObject();
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    // 其他可能的方法，例如，删除特定的键或合并SSTable等
}