package com.yovie;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;

// 命令行工具
public class DatabaseCLI {
    private DatabaseClient client;
    private Index index; // 添加索引实例
    private BufferedReader userInput;

    public static void main(String[] args) {
        try {
            DatabaseCLI databaseCLI = new DatabaseCLI();
            databaseCLI.start();
        } catch (IOException e) {
            System.err.println("Error: " + e.getMessage());
        }
    }
    public void start() throws IOException{
        try {
            client = new DatabaseClient("localhost", 8080);
            userInput = new BufferedReader(new InputStreamReader(System.in));
            index = new Index();

            while (true) {
                System.out.print("Enter command (CREATE/READ/UPDATE/DELETE/INDEX_ADD/INDEX_SEARCH/EXIT): ");
                String command = userInput.readLine().trim().toUpperCase();

                if ("EXIT".equals(command)) {
                    break;
                }

                switch (command) {
                    case "CREATE":
                        System.out.print("Enter key: ");
                        String key = userInput.readLine().trim();
                        System.out.print("Enter value: ");
                        String value = userInput.readLine().trim();
                        System.out.println(client.create(key, value));
                        break;
                    case "READ":
                        System.out.print("Enter key: ");
                        key = userInput.readLine().trim();
                        System.out.println(client.read(key));
                        break;
                    case "UPDATE":
                        System.out.print("Enter key: ");
                        key = userInput.readLine().trim();
                        System.out.print("Enter new value: ");
                        value = userInput.readLine().trim();
                        System.out.println(client.update(key, value));
                        break;
                    case "DELETE":
                        System.out.print("Enter key: ");
                        key = userInput.readLine().trim();
                        System.out.println(client.delete(key));
                        break;
                    case "INDEX_ADD":
                        addIndexEntry();
                        break;
                    case "INDEX_SEARCH":
                        searchIndex();
                        break;
                    default:
                        System.out.println("Invalid command. Please try again.");
                }
            }

            client.close();
        } catch (IOException | DatabaseException e) {
            System.err.println("Error: " + e.getMessage());
        }
    }
    private void addIndexEntry() throws IOException {
        System.out.print("Enter key for index: ");
        String key = userInput.readLine().trim();
        System.out.print("Enter position to add: ");
        long position = Long.parseLong(userInput.readLine().trim());
        index.addToIndex(key, position); // 添加到索引
        System.out.println("Index entry added.");
    }

    private void searchIndex() throws IOException {
        System.out.print("Enter key to search in index: ");
        String key = userInput.readLine().trim();
        List<Long> positions = index.search(key); // 搜索索引
        if (!positions.isEmpty()) {
            System.out.println("Found positions: " + positions);
        } else {
            System.out.println("No positions found for key: " + key);
        }
    }
}
