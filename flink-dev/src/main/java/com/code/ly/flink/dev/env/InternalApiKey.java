package com.code.ly.flink.dev.env;

/**
 * flink api 框架内部定义的配置关键字
 * @author luoy
 */
public class InternalApiKey {
    /** configuration **/
    public static final String FLINK_API_CONFIG_FILE_PATH = "flink.api.config.file";
    /** flink **/
    public static final String FLINK_API_PARALLELISM_KEY = "flink.api.parallelism";

    public static final String FLINK_API_CHECKPOINT_INTERVAL_KEY = "flink.api.checkpoint.interval";
    public static final String FLINK_API_CHECKPOINT_TIMEOUT_KEY = "flink.api.checkpoint.timeout";
    public static final String FLINK_API_CHECKPOINT_MODE_KEY = "flink.api.checkpoint.mode";

    /** kafka **/
    public static final String FLINK_API_KAFKA_TOPICS_KEY = "flink.api.kafka.topics";
    public static final String FLINK_API_KAFKA_CONSUMER_GROUP_KEY = "flink.api.kafka.consumer.group";

    public static final String FLINK_API_KAFKA_BOOTSTRAP_SERVERS_KEY = "flink.api.kafka.brokers";
    public static final String FLINK_API_KAFKA_OFFSET_RESET_KEY = "flink.api.kafka.offset.reset";

    //public static final String

}
