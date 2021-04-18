package com.code.ly.flink.stream.apply

import java.time.Duration

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object WindowUV {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment


        val stream = env.socketTextStream("",8080).map(str => {
            val list = str.split(",", -1)
            UserBehavior(list(0).toLong,list(1).toLong, list(2), list(3).toInt, list(4).toInt, list(5).toLong)
        })

        val windowedStream = stream.assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(10))
            .withTimestampAssigner(
                new SerializableTimestampAssigner[UserBehavior]() {
                    override def extractTimestamp(element: UserBehavior, recordTimestamp: Long): Long = element.time
                }
            )).map(x => (x.id, 1)).keyBy(x=> x._1).timeWindow(Time.hours(1)).sum(1)

        windowedStream.print("===>")
       env.execute("=== uv demo ====")

    }
}


case class UserBehavior(
    id:Long,
    time:Long,
    name:String,
    age:Int,
    behavior:Int,
    pageId:Long
)