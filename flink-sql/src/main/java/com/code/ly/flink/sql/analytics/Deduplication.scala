package com.code.ly.flink.sql.analytics

import com.code.ly.flink.sql.template.SqlCreateTemplate
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.types.Row

object Deduplication {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        val bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
        val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env, bsSettings)

        // 表定义
        tableEnv.executeSql(SqlCreateTemplate.createOrdersFromKafka("",""))

        // 需求说明：每个订单只保留最新的那条记录
        val sql ="""
                  | select
                  |     order_id,
                  |     order_time,
                  | from (
                  |     select id as order_id,
                  |             order_time,
                  |             row_number() over (partition by id order by order_time) as row_num
                  |     from orders
                  |     )
                  | )
                  | where row_num = 1
                  |""".stripMargin

        // Check for duplicates in the `orders` table,检查订单是否有重复
        val sql2 = """
                     |SELECT id AS order_id,
                     |       COUNT(*) AS order_cnt
                     |FROM orders o
                     |GROUP BY id
                     |HAVING COUNT(*) > 1;""".stripMargin
        val result = tableEnv.sqlQuery(sql)
        tableEnv.toAppendStream[Row](result).print("== deduplication demo  result ==")

        env.execute("deduplication demo")



    }
}
