package com.yovie;

import java.io.*;
import java.net.*;

public class DatabaseServer {
    private ServerSocket serverSocket;
    private Databasedb database;

    public DatabaseServer(int port) throws IOException {
        serverSocket = new ServerSocket(port);
        database = new Databasedb();
    }

    public void start() throws IOException {
        System.out.println("Database server started on port " + serverSocket.getLocalPort());
        while (true) {
            new ClientHandler(serverSocket.accept(), database).start();
        }
    }

    private static class ClientHandler extends Thread {
        private Socket clientSocket;
        private Databasedb database;
        private PrintWriter out;
        private BufferedReader in;

        public ClientHandler(Socket socket, Databasedb database) {
            this.clientSocket = socket;
            this.database = database;
        }

        public void run() {
            try {
                out = new PrintWriter(clientSocket.getOutputStream(), true);
                in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));

                String inputLine;
                while ((inputLine = in.readLine()) != null) {
                    String[] parts = inputLine.split(" ", 3);
                    String command = parts[0].toUpperCase();
                    String key = parts.length > 1 ? parts[1] : null;
                    String value = parts.length > 2 ? parts[2] : null;

                    switch (command) {
                        case "CREATE":
                            database.create(key, value);
                            out.println("Created: " + key);
                            break;
                        case "READ":
                            String readValue = database.read(key);
                            out.println(readValue != null ? readValue : "Key not found");
                            break;
                        case "UPDATE":
                            database.update(key, value);
                            out.println("Updated: " + key);
                            break;
                        case "DELETE":
                            database.delete(key);
                            out.println("Deleted: " + key);
                            break;
                        default:
                            out.println("Invalid command");
                    }
                }

                in.close();
                out.close();
                clientSocket.close();
            } catch (IOException e) {
                System.err.println("Error handling client: " + e.getMessage());
            }
        }
    }

    public static void main(String[] args) {
        try {
            DatabaseServer server = new DatabaseServer(8080);
            server.start();
        } catch (IOException e) {
            System.err.println("Could not start server: " + e.getMessage());
        }
    }
}