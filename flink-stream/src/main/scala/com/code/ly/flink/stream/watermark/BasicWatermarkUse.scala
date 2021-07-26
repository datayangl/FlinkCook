package com.code.ly.flink.stream.watermark

import java.time.Duration

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, Watermark, WatermarkGenerator, WatermarkOutput, WatermarkStrategy}
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

/**
 * watermark 的使用
 * 输入数据格式 样例： 100,hello  101,hi
 */
object BasicWatermarkUse {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment


        val stream = env.socketTextStream("",8080)
            .map(str => {
                val tokens=str.split(",")
                (tokens(0).toLong, tokens(1))
            })

        /** ---------------------- 内置 watermark策略 ----------------------- **/

        // 1.forMonotonousTimestamps  單調遞增時間戳場景策略
        val watermarkStream1 = stream.assignTimestampsAndWatermarks(WatermarkStrategy.forMonotonousTimestamps[(Long,String)].withTimestampAssigner(
            new SerializableTimestampAssigner[(Long,String)]() {
                override def extractTimestamp(element: (Long,String), recordTimestamp: Long): Long = element._1
            }
        ))

        //2.forBoundedOutOfOrderness  固定時延場景策略
        val watermarkStream2 = stream.assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(10)).withTimestampAssigner(
            new SerializableTimestampAssigner[(Long,String)]() {
                override def extractTimestamp(element: (Long,String), recordTimestamp: Long): Long = element._1
            }
        ))
    }

}


/** ------------------------ 自定义 watermark策略 -----------------------**/

object  MyBoundedOutOfOrdernessGenerator extends WatermarkGenerator[(Long,String)] {
    val maxOutOfOrderness = 600000 // 10分鐘

    var currentMaxTimestamp = Long.MinValue + maxOutOfOrderness + 1
    // 前一個Watermark時間戳
    var preWatermarkTimestamp = Long.MinValue

    override def onEvent(event: (Long, String), eventTimestamp: Long, output: WatermarkOutput): Unit = {
        currentMaxTimestamp = Math.max(currentMaxTimestamp, eventTimestamp)

    }

    override def onPeriodicEmit(output: WatermarkOutput): Unit = {
        val watermark = new Watermark(currentMaxTimestamp - maxOutOfOrderness - 1)
        val watermarkTimestamp = watermark.getTimestamp()

        preWatermarkTimestamp = watermarkTimestamp
        // 輸出Watermark
        output.emitWatermark(watermark)
    }
}

/*
* 自定义 Punctuated WatermarkGenerator
*/
object MyPunctuatedWatermarkGenerator extends WatermarkGenerator[(Long, String)] {

    override def onEvent(event: (Long, String), eventTimestamp: Long, output: WatermarkOutput): Unit = {
        val watermark = new Watermark(eventTimestamp)
        output.emitWatermark(watermark)
    }
    override def onPeriodicEmit(output: WatermarkOutput): Unit = {}
}

/**
 * 数据源产生watermark
 */
