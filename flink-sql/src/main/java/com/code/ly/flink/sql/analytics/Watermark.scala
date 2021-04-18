package com.code.ly.flink.sql.analytics

import com.code.ly.flink.sql.template.SqlCreateTemplate
import org.apache.flink.api.common.time.Time
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.types.Row

object Watermark {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        val bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
        val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env, bsSettings)

        // 表定义
        tableEnv.executeSql(SqlCreateTemplate.createOrdersFromKafka("",""))

        // 需求分析：统计每分钟的订单总数，允许乱序时间为15s
        val sql =
            """
              |select
              |     id,
              |     TUMBLE_START(order_time,INTERVAL '1' MINUTE) as window_start,
              |     count(*) as order_sum
              | from orders
              | group by
              |     TUMBLE(order_time, INTERVAL '1' MINUTE)
              |""".stripMargin

        val result = tableEnv.sqlQuery(sql)
        tableEnv.toAppendStream[Row](result).print("== watermark demo  result ==")

        env.execute("watermark demo")
    }
}
