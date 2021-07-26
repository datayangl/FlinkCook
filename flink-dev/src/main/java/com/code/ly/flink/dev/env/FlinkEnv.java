package com.code.ly.flink.dev.env;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.runtime.executiongraph.restart.RestartStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * flink 环境初始化工厂类
 * 1. 环境初始化：stream  batch
 * 2. checkpoint
 * 3. 重启策略
 * 4. statebackend
 * 5.
 *
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

    public FlinkEnv(Builder builder) {
        Mode mode = builder.mode;


    }
    /**
     *  初始化
     */

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



    public static class Builder{
        private long checkpointInterval;
        private int parallelism;
        private Mode mode;
        private RestartStrategy restartStrategy;
        private String stateBackend;

        public FlinkEnv build() {
            return new FlinkEnv(this);
        }

    }

}
