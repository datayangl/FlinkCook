package com.code.ly.flink.sql.join

import com.code.ly.flink.sql.template.SqlCreateTemplate
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

object LookupJoin {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        val bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
        val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env, bsSettings)

        tableEnv.execute(SqlCreateTemplate.createOrdersFromKafka("",""))
        tableEnv.execute(SqlCreateTemplate.createUsersFromMysql())

        //需求分析：关联用户表，打宽订单表


        // 如果mysql的表是时态表(版本表)，可以用如下sql
        val sql =
            """
              | select
              |     orders.*,
              |     u.user_name,
              |     u.age
              | from orders
              | join users FOR SYSTEM_TIME AS OF orders.order_time as u
              |     on orders.user_id = u.id
              |""".stripMargin


    }
}
