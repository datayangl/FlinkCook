package com.code.ly.flink.sql.analytics

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.types.Row

object TopN {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        val bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
        val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env, bsSettings)

        // 统计每小时总销售量前三的商品种类，比如食品、日用
        val sql =
            """select * from (
              | select * from row_number() over (partition by category order by cnt desc) as row_num
              | from (select TUMBLE_START(order_time,INTERVAL '1' MINUTE) as window_start,count(*) as cnt,category from orders group by TUMBLE(order_time,INTERVAL '1' MINUTE),category)
              |)
              |where row_num <=3
              |""".stripMargin

        val result = tableEnv.sqlQuery(sql)
        tableEnv.toAppendStream[Row](result).print("== topn demo result ==")

        env.execute("topn demo")
    }
}
