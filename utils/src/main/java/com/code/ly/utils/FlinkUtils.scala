package com.code.ly.utils

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{EnvironmentSettings, Table}
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.types.Row

object FlinkUtils {
    var streamEnv: StreamExecutionEnvironment = null
    var batchEnv: ExecutionEnvironment = null
    var streamTableEnv: StreamTableEnvironment = null
    var batchTableEnv: BatchTableEnvironment = null
    var streamBlinkSettings: EnvironmentSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()

    /**
     * 初始化Stream环境
     **/
    def initStream() = {
        streamEnv = StreamExecutionEnvironment.getExecutionEnvironment
        streamBlinkSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    }

    /**π
     * 初始化批处理环境
     **/
    def initBatch() = {
        batchEnv = ExecutionEnvironment.getExecutionEnvironment
    }

    /**
     * 初始化流处理Table环境
     **/
    def initStreamTableEnv() = {
        initStream()
        streamTableEnv = StreamTableEnvironment.create(streamEnv, streamBlinkSettings)

        streamTableEnv
    }

    /**
     * 初始化批处理Table环境
     */
    def initBatchTableEnv() = {
        initBatch()
        batchTableEnv = BatchTableEnvironment.create(batchEnv)
        batchTableEnv

    }
    /**
     * @param tableEnv ：table执行环境 ds:需要转换的DataStream格式
     *                 将DataStream转换为Table
     **/
    def dataStreamToTable(tableEnv: StreamTableEnvironment, ds: DataStream[Row]) = {
        tableEnv.fromDataStream(ds)
    }

    def arrayToRow(arr: Array[Any]) = {
        val row = new Row(arr.size)
        for (i <- 0.to(arr.size - 1)) {
            row.setField(i, arr(i))
        }

        row
    }

    /**
     * 追加模式，紧用于insert更新模式
     **/
    def tableToDataStreamInsert(tableEnv: StreamTableEnvironment, tb: Table) = {
        val ds: DataStream[Row] = tableEnv.toAppendStream[Row](tb)

        ds
    }

    /**``
     * 缩进模式 - 始终可以使用此模式，返回值是boolean类型。
     * 它用true或false来标记数据的插入和撤回，返回true代表数据插入，false代表数据的撤回
     **/
    def tableToDataStreamAll(tableEnv: StreamTableEnvironment, tb: Table) = {
        val ds: DataStream[(Boolean, Row)] = tableEnv.toRetractStream[Row](tb)

        ds
    }
    /***
     * @param table:Table表
     * @param tableEnv:Table执行环境
     * @param preIdentifier:打印输出前缀
     * @return void
     * */
    def tablePrint(table:Table,tableEnv:StreamTableEnvironment,preIdentifier:String): Unit ={
        val printSql = s"CREATE TABLE print_table WITH ('connector' = 'print','print-identifier' = '${preIdentifier}') LIKE ${table.toString} (EXCLUDING ALL)"
        tableEnv.sqlUpdate(printSql)
    }
    /***
     * @param table:Table表
     * @param tableEnv:Table执行环境
     * @return void``
     * */
    def tablePrint(table:Table,tableEnv:StreamTableEnvironment): Unit ={
        val printSql = s"CREATE TABLE print_table WITH ('connector' = 'print') LIKE ${table} (EXCLUDING ALL)"
        tableEnv.sqlUpdate(printSql)
    }



}