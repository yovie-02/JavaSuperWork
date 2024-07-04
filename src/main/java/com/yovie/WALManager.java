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

    public void logIndexOperation(String operation, String key, long position) throws IOException {
        String logEntry = "INDEX_OP," + operation + "," + key + "," + position + "\n";
        walStream.write(logEntry.getBytes());
        walStream.flush();
    }

    public void logSearchOperation(String operation, String key) throws IOException {
        String logEntry = "SEARCH_OP," + operation + "," + key + "\n";
        walStream.write(logEntry.getBytes());
        walStream.flush();
    }

    public void recover(Databasedb db) throws IOException {
        try (BufferedReader reader = new BufferedReader(new FileReader("wal.log"))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split(",");
                if (parts.length != 3) {
                    continue;
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

    public void recoverIndex(Index index) throws IOException {
        try (BufferedReader reader = new BufferedReader(new FileReader("wal.log"))) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.startsWith("INDEX_OP,")) {
                    String[] parts = line.substring("INDEX_OP,".length()).split(",");
                    if (parts.length == 3) {
                        String operation = parts[0];
                        String key = parts[1];
                        long position = Long.parseLong(parts[2]);

                        if ("INDEX_ADD".equals(operation)) {
                            index.read(key, position);
                        }
                        // 可以在这里添加更多的索引操作恢复逻辑
                    }
                } else if (line.startsWith("SEARCH_OP,")) {
                    String[] parts = line.substring("SEARCH_OP,".length()).split(",");
                    if (parts.length == 2) {
                        String operation = parts[0];
                        String key = parts[1];

                        if ("INDEX_SEARCH".equals(operation)) {
//                            index.search(key);
                            continue;
                        }
                    }
                }
            }
        }
    }

}