package com.code.ly.flink.dev.app;

import com.code.ly.flink.dev.config.ConfigManager;
import com.code.ly.flink.dev.env.FlinkStreamEnv;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;

public class AppBase {
    private static final Logger logger = LoggerFactory.getLogger(AppBase.class);

    /** 环境初始化 **/
    public void initEnv(Properties props){
        //FlinkStreamEnv env = new FlinkStreamEnv(props);
    }

    public Properties initConfig(String[] args) throws IOException {
        return ConfigManager.loadDefault(args);
    }

    public void beforeExecute(){

    }

    public void doExecute(StreamExecutionEnvironment env) {

    }

    public void start(StreamExecutionEnvironment env, String jobName) throws Exception {
        env.execute(jobName);
    }

    public void run(String[] args) throws IOException {
        logger.info("");
        // 1. 配置获取
        Properties props = initConfig(args);

        // 2. 环境初始化
        initEnv(props);

        //  开始执行

    }


}
