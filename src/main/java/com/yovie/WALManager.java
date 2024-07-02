package com.yovie;

import java.io.*;

public class WALManager {
    private FileOutputStream walStream;

    public WALManager() throws IOException {
        walStream = new FileOutputStream("wal.log", true); // 追加模式
    }

    public void logOperation(String operation, String key, String value) throws IOException {
        String logEntry = operation + "," + key + "," + (value != null ? value : "null") + "\n";
        walStream.write(logEntry.getBytes());
        walStream.flush();
    }

    public void recover(Databasedb db) throws IOException {
//        try (BufferedReader reader = new BufferedReader(new FileReader("wal.log"))) {
//            String line;
//            while ((line = reader.readLine()) != null) {
//                String[] parts = line.split(",");
//                if (parts.length != 3) {
//                    continue; // 跳过格式不正确的日志条目
//                }
//                String operation = parts[0];
//                String key = parts[1];
//                String value = parts[2];
//
//                switch (operation.toUpperCase()) {
//                    case "CREATE":
//                    case "UPDATE":
//                        db.create(key, value);
//                        break;
//                    case "DELETE":
//                        db.delete(key);
//                        break;
//                }
//            }
//        }
        try (BufferedReader reader = new BufferedReader(new FileReader("wal.log"))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split(",");
                if (parts.length != 3) {
                    continue; // 跳过格式不正确的日志条目
                }
                String operation = parts[0];
                String key = parts[1];
                String value = parts[2];

                switch (operation.toUpperCase()) {
                    case "CREATE":
                    case "UPDATE":
                        db.create(key, value);
                        break;
                    case "DELETE":
                        db.delete(key);
                        break;
                }
            }
        }
    }
}