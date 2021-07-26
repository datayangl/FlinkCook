package com.code.ly.flink.dev.demo;

import com.code.ly.flink.dev.config.ConfigManager;
import com.code.ly.flink.dev.env.FlinkEnv;
import com.code.ly.flink.dev.sql.udf.CodisLookupFunction;
import com.code.ly.flink.dev.sql.udf.PrefixKey;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.util.Properties;

public class RTDemo {
    public static void main(String[] args) throws Exception {
        FlinkEnv flinkEnv = new FlinkEnv(FlinkEnv.Mode.STREAM_TABLE);
        StreamExecutionEnvironment env = flinkEnv.getStreamEnv();
        StreamTableEnvironment tEnv = flinkEnv.getStreamTableEnv();

        args = new String[]{
            "--codis.zk.addr","192.168.10.36:2181",
            "--codis.zk.dir","/zk/codis/db_codis-demo/proxy"
        };

        Properties props = ConfigManager.loadFromArgs(args);
        tEnv.registerFunction("lookUpCodis", new CodisLookupFunction(props.getProperty("codis.zk.addr"), props.getProperty("codis.zk.dir")));
        tEnv.registerFunction("prefixId", new PrefixKey());

        tEnv.executeSql(TestSqls.kafka_sql);
        tEnv.executeSql(TestSqls.hbase_sql);

        String step1_sql = "select * from log";
        String step2_sql = "select lookUpCodis(prefixId(cid,'cid')) gid,ipv6 from log";
        String step3_sql = "insert into hTable select lookUpCodis(prefixId(cid,'cid')) row_key,Row(ipv6) from log";
//        Table table = tEnv.sqlQuery(step3_sql);
//
//        DataStream stream = tEnv.toAppendStream(table, TypeInformation.of(Row.class));
//
//        stream.print("===");
        tEnv.executeSql(step3_sql);

//        Table t = tEnv.sqlQuery(step1_sql);
//        DataStream stream = tEnv.toAppendStream(t, TypeInformation.of(Row.class));
//        stream.print("===");
//        env.execute("test");
    }
}
