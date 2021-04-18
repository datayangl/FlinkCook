package com.code.ly.flink.sql.join

import com.code.ly.flink.sql.template.SqlCreateTemplate
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.types.Row
import org.apache.flink.api.common.time.Time
object RegularJoin {
    val STATE_MIN_RETENSION_HOUR = 1L
    val STATE_MAX_RETENSION_HOUR = 24L

    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        val bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
        val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env, bsSettings)

        // 设置状态存储ttl，避免状态过大
        tableEnv.getConfig.setIdleStateRetentionTime(Time.hours(STATE_MIN_RETENSION_HOUR),Time.hours(STATE_MAX_RETENSION_HOUR))

        // 表定义
        tableEnv.executeSql(SqlCreateTemplate.createOrdersFromKafka("",""))
        tableEnv.executeSql(SqlCreateTemplate.createUsersFromKafka("",""))

        val sql = """ select * from order join user on order.user_id = user.id """

        val result = tableEnv.sqlQuery(sql)

        tableEnv.toAppendStream[Row](result).print("== regular join result ==")
        env.execute("regular join demo")
    }

}
