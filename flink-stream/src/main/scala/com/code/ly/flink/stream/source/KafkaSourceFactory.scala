package com.code.ly.flink.stream.source

import java.io.IOException
import java.util.Properties

import org.apache.flink.api.common.serialization.{AbstractDeserializationSchema, SimpleStringSchema}
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, KafkaDeserializationSchema}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.flink.streaming.api.scala._

/**
  *  kafka 常用的数据源算子抽取
  */
object KafkaSource {
    /**
      * 使用默认的SimpleStringSchema
      * @param topicList
      * @param properties
      */
    def kafkaStringSource(topicList:List[String], properties:Properties)= {
        import scala.collection.JavaConversions._
        val myConsumer = new FlinkKafkaConsumer[String](topicList, new SimpleStringSchema(), properties)
        myConsumer
    }

    /**
      * 使用自定义的 解序列化器，获取kafka的ConsumerRecord
      * @param topicList
      * @param properties
      * @return
      */
    def kafkaRecordSource(topicList:List[String], properties:Properties)= {
        import scala.collection.JavaConversions._
        val myConsumer = new FlinkKafkaConsumer[ConsumerRecord[Array[Byte], Array[Byte]]](topicList, new RecordDeserializationSchema[ConsumerRecord[Array[Byte], Array[Byte]]], properties)
        myConsumer
    }
    /**
      * 使用自定义的 解序列化器，获取kafka的ConsumerRecord，并将key value转成string
      * @param topicList
      * @param properties
      * @return
      */
    def kafkaRecordStringSource(topicList:List[String], properties:Properties)= {
        import scala.collection.JavaConversions._
        val myConsumer = new FlinkKafkaConsumer[ConsumerRecord[String,String]](topicList, new RecordKafkaSchema(), properties)
        myConsumer
    }
}
class RecordKafkaSchema extends KafkaDeserializationSchema[ConsumerRecord[String, String]] {
    override def isEndOfStream(nextElement: ConsumerRecord[String, String]): Boolean = false

    override def deserialize(record: ConsumerRecord[Array[Byte], Array[Byte]]): ConsumerRecord[String, String] = {
        var key: String = null
        var value: String = null
        if (record.key != null) {
            key = new String(record.key())
        }
        if (record.value != null) {
            value = new String(record.value())
        }
        new ConsumerRecord[String, String](
            record.topic(),
            record.partition(),
            record.offset(),
            record.timestamp(),
            record.timestampType(),
            record.checksum,
            record.serializedKeySize,
            record.serializedValueSize(),
            key,
            value)
    }

    override def getProducedType: TypeInformation[ConsumerRecord[String, String]] = TypeInformation.of(new TypeHint[ConsumerRecord[String, String]] {})

}

class RecordDeserializationSchema[T](implicit typeInformation: TypeInformation[T]) extends KafkaDeserializationSchema[T] {
    override def isEndOfStream(nextElement: T): Boolean = false

    override def deserialize(record: ConsumerRecord[Array[Byte], Array[Byte]]): T = record.asInstanceOf[T]

    override def getProducedType: TypeInformation[T] = typeInformation
}

