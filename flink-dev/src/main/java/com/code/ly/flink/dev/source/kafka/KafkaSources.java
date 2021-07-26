package com.code.ly.flink.dev.source.kafka;

import com.code.ly.flink.dev.env.InternalApiKey;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Properties;

/**
 * @author luoy
 */
public class KafkaSources {
    public static FlinkKafkaConsumer<String> kafkaStringSource(Properties props) {
        String topic = props.getProperty(InternalApiKey.FLINK_API_KAFKA_TOPICS_KEY);
        FlinkKafkaConsumer<String> myConsumer = new FlinkKafkaConsumer(topic, new SimpleStringSchema(), props);
        return myConsumer;
    }

    public static FlinkKafkaConsumer<ConsumerRecord<byte[], byte[]>> FlinkKafkaRecordConsumer(Properties props) {
        String topic = props.getProperty(InternalApiKey.FLINK_API_KAFKA_TOPICS_KEY);
        FlinkKafkaConsumer<ConsumerRecord<byte[], byte[]>> myConsumer = new FlinkKafkaConsumer(topic, new RecordDeserializationSchema(), props);
        return myConsumer;
    }

    public static FlinkKafkaConsumer FlinkKafkaCustomConsumer(Properties props, KafkaDeserializationSchema schema) {
        String topic = props.getProperty(InternalApiKey.FLINK_API_KAFKA_TOPICS_KEY);
        FlinkKafkaConsumer myConsumer = new FlinkKafkaConsumer(topic, schema, props);
        return myConsumer;
    }

}

class RecordDeserializationSchema implements KafkaDeserializationSchema<ConsumerRecord> {
    @Override
    public boolean isEndOfStream(ConsumerRecord consumerRecord) {
        return false;
    }

    @Override
    public ConsumerRecord<byte[], byte[]> deserialize(ConsumerRecord<byte[], byte[]> consumerRecord) throws Exception {
        return consumerRecord;
    }

    @Override
    public TypeInformation<ConsumerRecord> getProducedType() {
        return TypeInformation.of(ConsumerRecord.class);
    }
}