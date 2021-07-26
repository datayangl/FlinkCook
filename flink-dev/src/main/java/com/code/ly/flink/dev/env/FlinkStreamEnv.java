package com.code.ly.flink.dev.env;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.runtime.executiongraph.restart.RestartStrategy;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author luoy
 */
public class FlinkStreamEnv {
    Logger logger = LoggerFactory.getLogger(FlinkStreamEnv.class);
    private StreamExecutionEnvironment streamEnv = null;
    private StreamTableEnvironment streamTableEnv  = null;

}
