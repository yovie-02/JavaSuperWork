package com.yovie;

import java.io.*;
import java.net.*;

public class DatabaseClient {
    private Socket socket;
    private PrintWriter out;
    private BufferedReader in;

    public DatabaseClient(String host, int port) throws IOException {
        socket = new Socket(host, port);
        out = new PrintWriter(socket.getOutputStream(), true);
        in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
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
    public String create(String key, String value) throws IOException {
        return sendCommand("CREATE " + key + " " + value);
    }

    public String read(String key) throws IOException {
        return sendCommand("READ " + key);
    }

    public String update(String key, String value) throws IOException {
        return sendCommand("UPDATE " + key + " " + value);
    }

    public String delete(String key) throws IOException {
        return sendCommand("DELETE " + key);
    }
}

