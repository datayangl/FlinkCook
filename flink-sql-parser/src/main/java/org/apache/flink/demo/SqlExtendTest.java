package org.apache.flink.demo;

import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.flink.sql.parser.impl.FlinkSqlParserImpl;

public class SqlExtendTest {
    public static void main(String[] args) throws SqlParseException {
        String sql = "create table kafka_table(\n" +
                "ctime TIMESTAMP," +
                "cid  string," +
                "ipv6 string," +
                "WATERMARK FOR ctime AS withOffset(ctime, 1000)," +
                "proc AS PROCTIME()" +
                ") with (" +
                "'connector' = 'kafka'," +
                "'topic' = 'luoy-test'," +
                "'properties.bootstrap.servers' = '192.168.10.36:6667'," +
                "'properties.group.id' = 'luoy-test-consumer001'," +
                "'format' = 'json'," +
                "'scan.startup.mode' = 'latest-offset'" +
                ")";

        String sql2 = "select count(*) num, TUMBLE_START(proc, INTERVAL '5' MINUTE) ,TUMBLE_END(proc, INTERVAL '5' MINUTE) from  kafka_table GROUP BY TUMBLE(proc, INTERVAL '5' MINUTE)";
        SqlParser.Config config = SqlParser
                .configBuilder()
                .setParserFactory(FlinkSqlParserImpl.FACTORY)
                .setLex(Lex.JAVA)
                .build();

        SqlParser parser = SqlParser.create(sql2, config);
        SqlNode node = parser.parseStmt();
        System.out.println("finish");
    }
}
