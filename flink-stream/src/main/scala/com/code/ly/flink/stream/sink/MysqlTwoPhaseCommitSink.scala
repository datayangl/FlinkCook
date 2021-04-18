package com.code.ly.flink.stream.sink

import java.sql.{Connection, PreparedStatement, Timestamp}
import java.util.Date

import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.api.common.typeutils.base.VoidSerializer
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer
import org.apache.flink.streaming.api.functions.sink.{SinkFunction, TwoPhaseCommitSinkFunction}

/**
 *  mysql的二阶段提交sink实现
 * @param transactionSerializer
 * @param contextSerializer
 */
class MysqlTwoPhaseCommitSink(var transactionSerializer: TypeSerializer[Connection], var contextSerializer: TypeSerializer[Void]) extends TwoPhaseCommitSinkFunction[String, Connection, Void](transactionSerializer: TypeSerializer[Connection], contextSerializer: TypeSerializer[Void]){
    // 提供一个默认的辅助构造器
    def this() {
        this(new KryoSerializer(classOf[Connection], new ExecutionConfig()), VoidSerializer.INSTANCE)
    }

    override def invoke(connection: Connection, value: String, context: SinkFunction.Context[_]): Unit = {
        println("start invoke ......")
        val date = new Date().toString
        val sql = "insert into test ()"

        val ps = connection.prepareStatement(sql)
        ps.setString(1,value)
        ps.setTimestamp(2, new Timestamp(System.currentTimeMillis()))
        ps.execute()
        //手动产生异常
        if(Integer.parseInt(value) == 15) System.out.println(1/0);
    }

    /**
     * 获取连接后，开启手动提交事务
     * @return
     */
    override def beginTransaction(): Connection = {
        val url = "jdbc:mysql://localhost:3306/test?useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull&useSSL=false&autoReconnect=true";
        val connection = DBConnectUtil.getConnection(url,"user","password")
        println("start beginTransaction......."+connection)
        connection

    }

    override def preCommit(connection: Connection): Unit = {
       println("start preCommit......." + connection)
    }

    override def commit(connection: Connection): Unit = {
        println("start commit......." + connection)
        DBConnectUtil.commit(connection)
    }

    override def abort(connection: Connection): Unit = {
        println("start abort rollback......." + connection)
        DBConnectUtil.rollback(connection)
    }
}
