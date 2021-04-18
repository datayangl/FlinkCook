package com.code.ly.flink.sql.join

import com.code.ly.flink.sql.template.SqlCreateTemplate
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.types.Row

object IntervalJoin {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        val bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
        val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env, bsSettings)

        tableEnv.execute(SqlCreateTemplate.createOrdersFromKafka("",""))
        tableEnv.execute(SqlCreateTemplate.createShipmentsFromKafka("",""))

        val sql = """ select
                    |   o.id as order_id,
                    |   o.order_time,
                    |   s.shipment_time,
                    |   TIMESTAMPDIFF(DAY,o.order_time,s.shipment_time) AS day_diff
                    |from order o
                    |join  shipments s ON o.id = s.order_id
                    |WHERE
                    |    o.order_time BETWEEN s.shipment_time - INTERVAL '3' DAY AND s.shipment_time;
                    |""".stripMargin
        val result = tableEnv.sqlQuery(sql)
        tableEnv.toAppendStream[Row](result).print("== interval join result ==")
        env.execute("interval join demo")
    }
}
