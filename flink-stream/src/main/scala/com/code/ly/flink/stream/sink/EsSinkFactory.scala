package com.code.ly.flink.stream.sink

import java.net.SocketTimeoutException

import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.connectors.elasticsearch.{ActionRequestFailureHandler, ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink
import org.apache.flink.util.ExceptionUtils
import org.apache.http.HttpHost
import org.elasticsearch.ElasticsearchParseException
import org.elasticsearch.action.ActionRequest
import org.elasticsearch.client.Requests
import org.elasticsearch.common.unit.TimeValue
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException
import org.elasticsearch.common.xcontent.XContentType

/**
  * EsSink 的工厂类
  */
object EsSinkFactory {
    // 超时时间
    private val FLINK_ESSINK_WRITE_TIMEOUT: Long = 1000L

    // bulk最多写入数量
    private val SINK_ES_BULK_MAX = 1000

    // bulk写入的最大间隔时间
    private val ETL_SINK_ES_BULK_INTERVAL = 1000L

    /***
      * 添加es数据写入方法 - addSink(esSink)
      */
    def esSinMap(httpHost:List[HttpHost],esIndex:String)= {
        var httpHosts:Array[HttpHost] = Array()
        httpHost.foreach(x => {
            httpHosts = httpHosts.:+(x)
        })

        import scala.collection.JavaConversions._
        import collection.JavaConverters._
        val esSinkBuilder =  new ElasticsearchSink.Builder[Map[String,Any]](httpHosts.toSeq, new ElasticsearchSinkFunction[Map[String,Any]] {
            override def process(element: Map[String, Any], ctx: RuntimeContext, indexer: RequestIndexer): Unit = {
                //创建index request
                val indexRequest = Requests.indexRequest()
                    .index(esIndex)
                    .timeout(TimeValue.timeValueSeconds(FLINK_ESSINK_WRITE_TIMEOUT))
                    .source(element.asJava)
                //使用indexer发送请求，写入数据
                indexer.add(indexRequest)
            }
        })

        // es 支持的一些参数配置
        esSinkBuilder.setBulkFlushBackoff(true)
        esSinkBuilder.setBulkFlushBackoffRetries(100)
        esSinkBuilder.setBulkFlushMaxActions(SINK_ES_BULK_MAX)
        esSinkBuilder.setBulkFlushInterval(ETL_SINK_ES_BULK_INTERVAL)
        // es 写数据异常的处理
        esSinkBuilder.setFailureHandler(requestFailureHandler)
        esSinkBuilder.build()
    }

    def esSinkJson(httpHost:List[HttpHost],esIndex:String)={
        var httpHosts:Array[HttpHost] = Array()
        httpHost.foreach(x => {
            httpHosts = httpHosts.:+(x)
        })
        import scala.collection.JavaConversions._
        val esSinkBuilder: ElasticsearchSink.Builder[String] =  new ElasticsearchSink.Builder[String](httpHosts.toSeq, new ElasticsearchSinkFunction[String] {
            override def process(element: String, ctx: RuntimeContext, indexer: RequestIndexer): Unit = {
                //创建index request
                val indexRequest = Requests.indexRequest()
                    .index(esIndex)
                    .timeout(TimeValue.timeValueSeconds(FLINK_ESSINK_WRITE_TIMEOUT))
                    .source(element,XContentType.JSON)
                //使用indexer发送请求，写入数据
                indexer.add(indexRequest)
            }
        })
        // es 支持的一些参数配置
        esSinkBuilder.setBulkFlushBackoff(true)
        esSinkBuilder.setBulkFlushBackoffRetries(100)
        esSinkBuilder.setBulkFlushMaxActions(SINK_ES_BULK_MAX)
        esSinkBuilder.setBulkFlushInterval(ETL_SINK_ES_BULK_INTERVAL)
        // es 写数据异常的处理
        esSinkBuilder.setFailureHandler(requestFailureHandler)

        esSinkBuilder.build()
    }

    /**
      *  es 异常处理
      * @return
      */
    def requestFailureHandler()={
        val requestFialureHandler: ActionRequestFailureHandler = new ActionRequestFailureHandler {
            override def onFailure(action: ActionRequest, failure: Throwable, restStatusCode: Int, indexer: RequestIndexer): Unit = {
                //ES异常处理1：队列满了(Reject异常)，放回队列
                if(ExceptionUtils.findThrowable(failure,classOf[EsRejectedExecutionException]).isPresent){
                    indexer.add(action)
                    println("-|触发Flink sink es 队列已满异常.")
                }else if(ExceptionUtils.findThrowable(failure,classOf[SocketTimeoutException]).isPresent){
                    //ES异常处理2：ES超时异常(timeout),放回队列
                    indexer.add(action)
                    println("-|触发Flink sink es timeout 异常.")
                }else if(ExceptionUtils.findThrowable(failure,classOf[ElasticsearchParseException]).isPresent){
                    //ES异常处理3：ES语法异常，丢弃数据，记录日志
                    println("-| ES数据异常",action.toString(),"")
                }else {
                    //ES异常处理4：ES其它异常，丢弃数据，记录日志
                    throw new Exception("")
                }
            }
        }

        requestFialureHandler
    }
}
