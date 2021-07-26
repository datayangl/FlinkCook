package com.code.ly.flink.stream.pojo

import scala.collection.mutable.ArrayBuffer

case class Rule(
   name:String,      //规则名称
   rule_id:String,   //泛化规则id
   rule_enable:Boolean,  //规则是否启用
   dev_type:String,      //设备类型
   regexStr:String,      //正则匹配字符串

   keyword_enable: Boolean, //关键字字段是否保留
   keyword:Seq[String],     //关键字
   keyword_strategy:Boolean,

   generalize_detail_enable:Boolean,           //泛化规则是否启用
   generalize_detail:Seq[Map[String,String]], //泛化映射字段List[Map("index"->"0", "field"-> "time", "strategy":"reflect", "detail":"")

   generalize_extra_enable:Boolean,         // 泛化补充字段是否启用
   generalize_extra:Map[String,String],     //泛化补充字段

   reflect:Map[String, Map[String,String]],  //字段映射维表[维表id, [原始字段值, 映射字段值]]

   merge_enable:Boolean,       //数据归并是否启用
   merge_fields:Seq[String],   //数据归并判断字段
   merge_time:Long          //数据归并周期
)
{
    // 重写toString,方便打印
    override def toString: String = {
        val str =
            s"""Rule:{name:${name}, rule_id:${rule_id}, rule_enable:${rule_enable}, dev_type:${dev_type}, regexStr:${regexStr},
               |keyword_enable:${keyword_enable}, keyword:${keyword}, keyword_strategy:${keyword_strategy},
               |generalize_detail_enable:${generalize_detail_enable}, generalize_detail:${generalize_detail},
               |generalize_extra_enable:${generalize_extra_enable}, generalize_extra:${generalize_extra},
               |reflect:${reflect},
               |merge_enable:${merge_enable}, merge_fields:${merge_fields}, merge_time:${merge_time}
               |}""".stripMargin
        str
    }
}

object Rule {
    /**
     * 判断正则匹配
     * @param str
     * @return
     */
    def regexMatch(str:String, regexStr:String): Boolean = {
        val regex = regexStr.r
        val isMatch = str match {
            case regex(_*) => true
            case _ => false
        }
        isMatch
    }


    /**
     * 正则解析字符串提取组
     * @param str
     * @return
     */
    def regexExtract(str:String, regexStr:String) = {
        val regex = regexStr.r
        val buffer = ArrayBuffer[String]()
        val regex(fields @ _*) = str
        for (group <- fields) {
            buffer += group
        }
        buffer
    }

    /**
     * 通知规则解析
     * @param msg 通知的更新规则
     */
    def parseStr2Rule(msg:String) = {
        // 省略规则解析
        null
    }

}
