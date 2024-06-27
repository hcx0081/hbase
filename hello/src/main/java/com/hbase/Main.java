package com.hbase;

public class Main {
    public static void main(String[] args) {
        // HBaseUtils.deleteTable("test", "testTable");
        // HBaseUtils.createTable("test", "testTable", "info");
        // HBaseUtils.put("test", "testTable", "1000", "info", "name", "zs");
        
        // List<String> list = HBaseUtils.get("test", "testTable", "1000", "info", "name");
        // list.forEach(System.out::println);
        
        // List<String> list = HBaseUtils.scan("test", "testTable", "0", "10000");
        // list.forEach(System.out::println);
        // HBaseUtils.delete("test", "testTable", "1000", "info", "name");
        //
        
        System.out.println(HBaseUtils.getConnection());
        HBaseUtils.closeConnection();
        System.out.println(HBaseUtils.getConnection());
        HBaseUtils.createTable("test", "testTable111", "info");
        System.out.println(HBaseUtils.getConnection());
    }
}