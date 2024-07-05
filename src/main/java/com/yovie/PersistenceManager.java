package com.yovie;

import java.io.File;
import java.io.FileOutputStream;
import java.io.*;

public class PersistenceManager {
    private static final long MAX_FILE_SIZE = 512;
//    private File currentFile;
    private File walFile;
    private FileOutputStream fos;

    public PersistenceManager() throws IOException {
        walFile = new File("wal.log");
    }

    public void write(String key, String value) throws IOException {
        if (walFile.length() > MAX_FILE_SIZE) {
            rotate();
        }
    }

    private void rotate() throws IOException {
        File newFile = new File("wal_" + System.currentTimeMillis() + ".log");
        walFile.renameTo(newFile);
        walFile = new File("wal.log");
        fos = new FileOutputStream(walFile, true);
        CompressTask.submit(newFile);
    }
}