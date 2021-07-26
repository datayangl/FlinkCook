package com.code.ly.flink.stream.env;


import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * @author luoy
 */
public class FlinkEnv {
    Logger logger = LoggerFactory.getLogger(FlinkEnv.class);
    private StreamExecutionEnvironment streamEnv = null;
    private ExecutionEnvironment batchEnv  = null;

    private StreamTableEnvironment streamTableEnv  = null;
    private TableEnvironment batchTableEnv = null;

    public enum Mode {
        STREAM,
        STREAM_TABLE,
        BATCH,
        BATCH_TABLE
    }

    public FlinkEnv(Mode mode) {
        logger.info("initialize flink with mode {}", mode.name());
        switch (mode) {
            case STREAM:
                initStream();
                break;
            case STREAM_TABLE:
                initStreamTableEnv();
                break;
            case BATCH:
                initBatch();
                break;
            case BATCH_TABLE:
                initBatchTableEnv();
                break;
            default:
                logger.error("unsupported mode");
                throw new IllegalArgumentException("unsupported mode");
        }
    }

    /** use blinkPlanner(default) **/
    EnvironmentSettings streamBlinkSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();

    EnvironmentSettings batchBlinkSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build();

    /**
     * 初始化 stream 环境
     **/
    public void initStream() {
        streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
    }

    /**π
     * 初始化 batch 环境
     **/
    public void initBatch() {
        batchEnv = ExecutionEnvironment.getExecutionEnvironment();
    }

    /**
     * 初始化 stream table环境
     **/
    public void initStreamTableEnv() {
        initStream();
        streamTableEnv = StreamTableEnvironment.create(streamEnv, streamBlinkSettings);
    }

    /**
     * 初始化 batch table 环境
     */
    public void initBatchTableEnv() {
        initBatch();
        batchTableEnv = TableEnvironment.create(batchBlinkSettings);
    }


    /**
     * 初始化 checkpoint 环境
     */
    public void setCheckpointEnv() {

    }

    /** only getter **/
    public StreamExecutionEnvironment getStreamEnv() {
        preCheckState(streamEnv);
        return streamEnv;
    }


    public ExecutionEnvironment getBatchEnv() {
        preCheckState(batchEnv);
        return batchEnv;
    }

    public StreamTableEnvironment getStreamTableEnv() {
        preCheckState(streamTableEnv);
        return streamTableEnv;
    }

    public TableEnvironment getBatchTableEnv() {
        preCheckState(batchTableEnv);
        return batchTableEnv;
    }

    private void preCheckState(Object t) {
        if (t == null) {
            throw new IllegalStateException(String.format("%s has not been initialized", t.getClass().getName()));
        }
    }
}
