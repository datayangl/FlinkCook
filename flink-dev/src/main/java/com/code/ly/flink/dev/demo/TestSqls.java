package com.code.ly.flink.dev.demo;

public class TestSqls {
    public static String kafka_sql = "create table log(" +
            "cid  string," +
            "ipv6 string" +
            ") with (" +
            "'connector' = 'kafka'," +
            "'topic' = 'luoy-test'," +
            "'properties.bootstrap.servers' = '192.168.10.36:6667'," +
            "'properties.group.id' = 'luoy-test-consumer001'," +
            "'format' = 'json'," +
            "'scan.startup.mode' = 'latest-offset'" +
            ")";


    public static String hbase_sql = "CREATE TABLE hTable (" +
            " row_key String," +
            " inf ROW<ip String>," +
            " PRIMARY KEY (row_key) NOT ENFORCED" +
            ") WITH (" +
            " 'connector' = 'hbase-1.4'," +
            " 'table-name' = 'test:ids_2_gid'," +
            " 'zookeeper.quorum' = '192.168.10.36:2181'" +
            ")";
//    public static String hbase_sql = "CREATE TABLE hTable (" +
//            " row_key String," +
//            " inf ROW<ip String>," +
//            " PRIMARY KEY (row_key) NOT ENFORCED" +
//            ") WITH (" +
//            " 'connector.type' = 'hbase'," +
//            " 'connector.version' = '1.4.3'," +
//            " 'connector.table-name' = 'test:ids_2_gid'," +
//            " 'connector.zookeeper.quorum' = '192.168.10.36:2181'" +
//            ")";
    public static void main(String[] args) {
        System.out.println(kafka_sql);
        System.out.println(hbase_sql);
    }

}
