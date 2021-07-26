package com.code.ly.flink.dev.env;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.runtime.executiongraph.restart.RestartStrategy;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * @author luoy
 */
public class FlinkBatchEnv {
    Logger logger = LoggerFactory.getLogger(FlinkBatchEnv.class);
    private ExecutionEnvironment batchEnv  = null;

    private TableEnvironment batchTableEnv = null;

    /** 默认使用blink **/
    private static final EnvironmentSettings batchBlinkSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build();

    public enum BatchMode {
        BATCH,
        BATCH_TABLE
    }

    public FlinkBatchEnv(FlinkBatchEnv.BatchMode mode) {
        switch (mode) {
            case BATCH:
                initBatchEnv();
                break;
            case BATCH_TABLE:
                initBatchEnv();
                initBatchTableEnv();
                break;
            default:
                logger.error("unsupported mode");
                throw new IllegalArgumentException("unsupported mode");
        }
    }

    private FlinkBatchEnv(FlinkBatchEnv.BatchMode mode, Properties properties) {
        switch (mode) {
            case BATCH:
                initBatchEnv();
                break;
            case BATCH_TABLE:
                initBatchEnv();
                initBatchTableEnv();
                break;
            default:
                logger.error("unsupported mode");
                throw new IllegalArgumentException("unsupported mode");
        }
    }

    /**π
     * 初始化 batch 环境
     **/
    public void initBatchEnv() {
        batchEnv = ExecutionEnvironment.getExecutionEnvironment();
    }

    /**
     * 初始化 batch 环境
     **/
    public void initBatchEnv(Properties props) {
        batchEnv = ExecutionEnvironment.getExecutionEnvironment();
    }

    public void initBatchTableEnv() {
        batchTableEnv = TableEnvironment.create(batchBlinkSettings);
    }

    public ExecutionEnvironment getBatchEnv() {
        return batchEnv;
    }

    public TableEnvironment getBatchTableEnv() {
        return batchTableEnv;
    }


    public void setBatchEnv() {
    }

    public FlinkBatchEnv(Builder builder) {

    }

    public static class Builder{
        public FlinkBatchEnv build() {
            return new FlinkBatchEnv(this);
        }
    }
}
