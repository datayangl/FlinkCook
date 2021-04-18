package com.code.ly.flink.stream.trigger

import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

/**
 * 到达指定数量 或者 到达指定窗口时间时触发
 *
 * @param threshold 触发值
 * @tparam T
 */
class CountTrigger[T](threshold:Int) extends  Trigger[T, TimeWindow]{
    var count = 0
    override def onElement(element: T, timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
        ctx.registerEventTimeTimer(window.maxTimestamp())

        count = count + 1
        println(s"--------------------------  current count is ${count} --------------------------")
        val result = if (count > threshold) {
            count = 0
            ctx.deleteProcessingTimeTimer(window.maxTimestamp())
            TriggerResult.FIRE_AND_PURGE
        } else TriggerResult.CONTINUE

        result
    }

    override def onProcessingTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
        val result = if (count > 0) {
            count = 0
            TriggerResult.FIRE_AND_PURGE
        } else TriggerResult.CONTINUE
        result
    }

    override def onEventTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
        val result = if (time >= window.maxTimestamp() && count > 0) {
            count = 0
            TriggerResult.FIRE_AND_PURGE
        } else if (time >= window.maxTimestamp() && count == 0) {
            TriggerResult.PURGE
        } else TriggerResult.CONTINUE
        result
    }

    override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = {
        ctx.deleteProcessingTimeTimer(window.maxTimestamp())
        ctx.deleteEventTimeTimer(window.maxTimestamp())
    }
}
