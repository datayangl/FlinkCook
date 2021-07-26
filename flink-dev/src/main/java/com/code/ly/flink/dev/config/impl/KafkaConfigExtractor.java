package com.code.ly.flink.dev.config.impl;

import com.code.ly.flink.dev.config.ConfigExtractor;
import com.code.ly.flink.dev.env.InternalApiKey;
import com.code.ly.flink.dev.util.CheckConditions;

import java.util.Properties;

/**
 * @author luoy
 */
public class KafkaConfigExtractor implements ConfigExtractor<Properties> {
    @Override
    public Properties extract(Properties config) {
        validate(config);
        Properties props = new Properties();
        props.put("bootstrap.servers", config.getProperty(InternalApiKey.FLINK_API_KAFKA_BOOTSTRAP_SERVERS_KEY, ""));
        props.put("group.id", config.getProperty(InternalApiKey.FLINK_API_KAFKA_TOPICS_KEY, ""));
        props.put("", config.getProperty(InternalApiKey.FLINK_API_KAFKA_CONSUMER_GROUP_KEY, ""));
        props.put("",config.getProperty(InternalApiKey.FLINK_API_KAFKA_OFFSET_RESET_KEY, "latest"));

        return props;

    }

    public void validate(Properties config) {
        CheckConditions.checkState(config.containsKey(InternalApiKey.FLINK_API_KAFKA_TOPICS_KEY),"flink.api.kafka.topics is not configured");
        CheckConditions.checkState(config.containsKey(InternalApiKey.FLINK_API_KAFKA_BOOTSTRAP_SERVERS_KEY),"flink.api.kafka.brokers is not configured");
        CheckConditions.checkState(config.containsKey(InternalApiKey.FLINK_API_KAFKA_CONSUMER_GROUP_KEY),"flink.api.kafka.consumer.group is not configured");
    }

}
