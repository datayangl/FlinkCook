package com.code.ly.flink.stream.config;

import org.apache.flink.api.java.utils.ParameterTool;

import java.io.IOException;

/**
 * 配置加载 system args file hdfs-file
 */
public class ConfigLoader {

    /**
     *
     */
    public void loadFromSystem() {
    }


    public ParameterTool  loadWithPriority(String[] args) throws IOException {
        return ParameterTool
                .fromPropertiesFile("")
                .mergeWith(ParameterTool.fromArgs(args))
                .mergeWith(ParameterTool.fromSystemProperties());
    }
}
