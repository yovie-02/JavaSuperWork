package com.yovie;

import java.io.File;
import java.io.FileOutputStream;
import java.io.*;

public class PersistenceManager {
    private static final long MAX_FILE_SIZE = 512;
    private File currentFile;
    private File walFile;
    private FileOutputStream fos;

    public PersistenceManager() throws IOException {
        currentFile = new File("current.db");
        walFile = new File("wal.log");
        fos = new FileOutputStream(currentFile, true);
    }

    public void write(String key, String value) throws IOException {
        String entry = key + ":" + (value != null ? value : "null") + "\n";
        fos.write(entry.getBytes());
        if (currentFile.length() > MAX_FILE_SIZE) {
            rotate();
        }
        if (walFile.length() > MAX_FILE_SIZE) {
            rotate();
        }
    }

    private void rotate() throws IOException {
        fos.close();
        File newFile = new File("data_" + System.currentTimeMillis() + ".db");
        currentFile.renameTo(newFile);
        currentFile = new File("current.db");
        fos = new FileOutputStream(currentFile, true);
        CompressTask.submit(newFile);
    }
}