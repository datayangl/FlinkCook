package com.code.ly.flink.stream.sink

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Admin, Connection, ConnectionFactory, Put, Table}
import org.apache.hadoop.hbase.util.Bytes

/**
  * HBase sink工厂类
  */
object HBaseSink {
    def hbaseSink() = {
        HBaseSink
    }
}

case  class HBDATA(tableName:String,rowkey:String,cf:String,fieldsValue:Map[String,String])
class HBaseSink extends RichSinkFunction[HBDATA] with Serializable {

    var connection: Connection = null
    var admin: Admin = null
    var hTable: Table = null
    var putList: List[Put] = List()

    val HBASE_ZOOKEEPER_PATH = ""
    val HBASE_ZOOKEEPER_QUORUM = ""
    val HBASE_ZOOKEEPER_PORT = "8023"

    override def open(parameters: Configuration): Unit = {
        super.open(parameters)
        val configuration = HBaseConfiguration.create()
        configuration.set("zookeeper.znode.parent", HBASE_ZOOKEEPER_PATH)
        configuration.set("hbase.zookeeper.quorum", HBASE_ZOOKEEPER_QUORUM)
        configuration.set("hbase.zookeeper.property.clientPort", HBASE_ZOOKEEPER_PORT)
        connection = ConnectionFactory.createConnection(configuration)
        admin = connection.getAdmin
    }

    override def invoke(data: HBDATA, context: SinkFunction.Context[_]): Unit = {
        hTable = connection.getTable(TableName.valueOf(data.tableName))
        /*val curTime = System.currentTimeMillis()
        val df = new SimpleDateFormat("yyyyMMddHHmmss")
        val strTime = df.format(curTime)*/
        val key = data.rowkey
        val columnFamily = data.cf
        val put = new Put(Bytes.toBytes(key))
        data.fieldsValue.foreach(dv => {
            put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(dv._1), Bytes.toBytes(dv._2.toString))
        })
        hTable.put(put)
    }


    override def close(): Unit = {
        if (hTable != null) hTable.close()
        super.close()
        if (connection != null) connection.close()
    }
}
