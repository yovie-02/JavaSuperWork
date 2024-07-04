package com.yovie;

import java.io.*;
import java.net.*;

public class DatabaseClient {
    private Socket socket;
    private PrintWriter out;
    private BufferedReader in;

    private LSMTree lsmTree;
    private WALManager walManager;
    private PersistenceManager persistenceManager;

    public DatabaseClient(String host, int port) throws IOException {
        socket = new Socket(host, port);
        out = new PrintWriter(socket.getOutputStream(), true);
        in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        lsmTree = new LSMTree();
        walManager = new WALManager();
        persistenceManager = new PersistenceManager();
    }

    public String sendCommand(String command) throws IOException {
        out.println(command);
        return in.readLine();
    }

    public void close() throws IOException {
        in.close();
        out.close();
        socket.close();
    }

    // API 方法
    public String create(String key, String value) throws IOException, DatabaseException {
        try {
            walManager.logOperation("CREATE", key, value);
            lsmTree.put(key, value);
            persistenceManager.write(key, value);
        } catch (IOException e) {
            throw new DatabaseException("Failed to create: " + e.getMessage());
        }
        return sendCommand("CREATE " + key + " " + value);
    }

    public String read(String key) throws IOException, DatabaseException {
        try {
            walManager.logOperation("READ", key, null);
            lsmTree.get(key);
            persistenceManager.write(key, null);
        } catch (Exception e) {
            throw new DatabaseException("Failed to read: " + e.getMessage());
        }
        return sendCommand("READ " + key);
    }

    public String update(String key, String value) throws IOException, DatabaseException {
        try {
            walManager.logOperation("UPDATE", key, value);
            lsmTree.put(key, value);
            persistenceManager.write(key, value);
        } catch (IOException e) {
            throw new DatabaseException("Failed to update: " + e.getMessage());
        }
        return sendCommand("UPDATE " + key + " " + value);
    }

    public String delete(String key) throws IOException, DatabaseException {
        try {
            walManager.logOperation("DELETE", key, null);
            lsmTree.delete(key);
            persistenceManager.write(key, null); // 写入 null 表示删除操作
        } catch (IOException e) {
            throw new DatabaseException("Failed to delete: " + e.getMessage());
        }
        return sendCommand("DELETE " + key);
    }
}

