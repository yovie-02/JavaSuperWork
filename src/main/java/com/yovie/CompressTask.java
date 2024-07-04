package com.yovie;

import java.io.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.zip.GZIPOutputStream;

public class CompressTask implements Runnable {
    private static ExecutorService executor = Executors.newFixedThreadPool(5);
    private File file;
    private WALManager walManager;

    public CompressTask(File file) {
        this.file = file;
        run();
    }

    public static void submit(File file) {
        executor.submit(new CompressTask(file));
    }

    @Override
    public void run() {
        try (GZIPOutputStream gzos = new GZIPOutputStream(new FileOutputStream(file.getName() + ".gz"));
             FileInputStream fis = new FileInputStream(file)) {
            byte[] buffer = new byte[1024];
            int len;
            while ((len = fis.read(buffer)) > 0) {
                gzos.write(buffer, 0, len);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            file.delete();
            try {
                walManager = new WALManager();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}