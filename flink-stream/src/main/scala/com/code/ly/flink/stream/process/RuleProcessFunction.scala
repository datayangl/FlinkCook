package com.code.ly.flink.stream.process

import com.code.ly.flink.stream.pojo.Rule
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.api.common.typeinfo.{TypeInformation, Types}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction
import org.apache.flink.util.Collector


/**
 * 自定义BroadcastProcessFunction
 * String: 第一个流(事件流)的数据类型
 * Rule: 第二个流(配置流)的数据类型
 * Map[String, Any]: 返回的数据类型
 *
 *
 * 适合场景：规则数据量不大且更新频率较低
 */
class RuleProcessFunction extends BroadcastProcessFunction[String, Rule, String ]{

    var initialCache:CacheRules = _

    /** 定义MapStateDescriptor */
    //val configDescriptor = new MapStateDescriptor("config", Types.VOID, Types.MAP(Types.STRING, TypeInformation.of(classOf[AnyRef])))
    val ruleDescriptor = new MapStateDescriptor("etl_rules",
        //Key类型
        Types.VOID,
        //Value类型
        TypeInformation.of(classOf[CacheRules]))

    // 初始阶段,数据库拉取规则数据进行初始化
    override def open(parameters: Configuration): Unit = {
        println("------------------ 初始化获取规则 -----------------------")
        // 省略具体从数据库拉取规则，并缓存到initialCache的逻辑
        val rules:Seq[Rule]= Seq[Rule]()
        val ruleMap = Map[String, Rule]()
        println("{initial rule cache}:" + ruleMap)
        initialCache = CacheRules(ruleMap)
    }

    override def processElement(value: String, ctx: BroadcastProcessFunction[String, Rule, String]#ReadOnlyContext, out: Collector[String]): Unit = {
        val broadcastState = ctx.getBroadcastState(ruleDescriptor)
        // 获取缓存的规则
        val rulesCache = if (broadcastState.get(null) == null) initialCache else broadcastState.get(null)
        // 缓存规则和日志进行匹配，省略规则匹配逻辑，返回结果为result
        val result = ""
        out.collect(result)
    }

    override def processBroadcastElement(value: Rule, ctx: BroadcastProcessFunction[String, Rule, String]#Context, out: Collector[String]): Unit = {
        val broadcastState = ctx.getBroadcastState(ruleDescriptor)
        var rulesCache = broadcastState.get(null)
        // 如果广播
        if (rulesCache == null) {
            println("use initial cache")
            println("initial cache is " + initialCache)
            rulesCache = initialCache
        }
        val newRulesCache = rulesCache match {
            // 如果为空，则说明数据库并没有持久化的规则，则创建新的空规则缓存，并写入第一条规则
            case null => {
                CacheRules( Map[String, Rule]( ""-> value))
            }
            // 不为空
            case CacheRules(rules) => {
                val newRules:CacheRules  = rulesCache
                newRules
            }
        }

        broadcastState.clear()
        broadcastState.put(null, newRulesCache)
    }
}

case class CacheRules(
    rules:  Map[String, Rule] // 缓存的规则   rule_id -> Rule
) {
    override def toString: String = {
        s"""{rules=${rules}}"""
    }
}