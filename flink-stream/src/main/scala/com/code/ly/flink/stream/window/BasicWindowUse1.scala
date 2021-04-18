package com.code.ly.flink.stream.window

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * flink 1.11 推荐窗口api
 *
 * flink中window按照上游是否是KeyedStream 划分为 Global Window 和 KeyedWindow
 *
 * 基于业务数据的方面考虑,Flink又支持两种类型的窗口,一种是基于时间的窗口叫Time Window,
 * 还有一种基于输入数据量的窗口叫Count Window
 */
object BasicWindowUse1 {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment


        val stream = env.socketTextStream("",8080)
            .map(str => {
                val tokens=str.split(",")
                (tokens(0), tokens(1).toLong)
            })

        /**
         * Time Window
         * 根据不同的业务场景,Time Window也可以分为三种类型,分别是滚动窗口(Tumbling Window)、滑动窗口(Sliding Window)和会话窗口叫Count Window
         */
        /** ----------------------------------  keyed window ----------------------------- **/

        // 1.滚动窗口 process time
        val windowStream1 = stream.keyBy(x => x._1).timeWindow(Time.seconds(5)).sum(1)


        // 2。滑动窗口
        val windowStream20 = stream.keyBy(x => x._1).timeWindow(Time.seconds(10), Time.seconds(5)).sum(1)

        val sessionWindow = stream.keyBy(x => x._1).window(EventTimeSessionWindows.withGap(Time.seconds(3)))


        /** ----------------------------------  global window ----------------------------- **/

        //val globalWindowStream = stream.windowAll()
    }

}
