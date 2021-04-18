package com.code.ly.flink.stream.window

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.{ProcessingTimeSessionWindows, SlidingEventTimeWindows, SlidingProcessingTimeWindows, TumblingEventTimeWindows, TumblingProcessingTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.scala._

/**
 * flink1.12推荐api
 */
object BasicWindowUse2 {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment

        val stream = env.socketTextStream("",8080)
            .map(str => {
                val tokens=str.split(",")
                (tokens(0), tokens(1).toLong)
            })

        /** ----------------------------------  keyed window ----------------------------- **/

        // 1.滚动窗口 process time
        val windowStream11 =  stream.keyBy(x => x._1).window(TumblingProcessingTimeWindows.of(Time.seconds(5)))

        val windowStream12 =  stream.keyBy(x => x._1).window(TumblingEventTimeWindows.of(Time.seconds(5)))

        // 2。滑动窗口
        val windowStream21 = stream.keyBy(x => x._1).window(SlidingProcessingTimeWindows.of(Time.seconds(5), Time.seconds(1)))

        val windowStream22 = stream.keyBy(x => x._1).window(SlidingEventTimeWindows.of(Time.seconds(5), Time.seconds(1)))

        // 3.会话窗口
        val windowStream31 = stream.keyBy(x => x._1).window(ProcessingTimeSessionWindows.withGap(Time.seconds(5)))

        // 4.计数窗口
        val windowStream41 = stream.keyBy(x => x._1).countWindow(10).sum(1)

        val windowStream42 = stream.keyBy(x => x._1).countWindow(10,5).sum(1)

        //5.

    }

}
