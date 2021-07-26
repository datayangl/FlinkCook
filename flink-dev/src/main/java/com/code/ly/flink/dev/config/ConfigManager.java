package com.code.ly.flink.dev.config;

import com.code.ly.flink.dev.util.StringUtil;
import org.apache.flink.api.java.utils.ParameterTool;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Properties;

import static com.code.ly.flink.dev.env.InternalApiKey.FLINK_API_CONFIG_FILE_PATH;

public class ConfigManager {

    /**
     * args 加载配置
     */
    public static Properties loadFromArgs(String[] args) {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        return parameterTool.getProperties();
    }

    /**
     *  system 加载
     */
    public static Properties loadFromSystem() {
        ParameterTool parameterTool = ParameterTool.fromSystemProperties();
        return  parameterTool.getProperties();
    }

    /**
     *  本地文件 绝对路径 加载
     */
    public static Properties loadFromAbsoluteFile(String filePath) throws IOException {
        ParameterTool parameterTool = ParameterTool.fromPropertiesFile(filePath);
        return  parameterTool.getProperties();
    }

    /**
     *  hdfs 文件加载
     */
    public static Properties loadFromHdfsFile(String hdfsPath) {
        return HdfsFileLoader.loadFile2Properties(hdfsPath);
    }

    /**
     * 默认配置加载优先级  file > args > system
     * @return
     */
    public static Properties loadDefault(String[] args) throws IOException {
        ParameterTool tool = ParameterTool.fromSystemProperties();

        if (args != null) {
            tool = tool.mergeWith(ParameterTool.fromArgs(args));
        }

        if (tool.has(FLINK_API_CONFIG_FILE_PATH)) {
            String filePath = tool.get(FLINK_API_CONFIG_FILE_PATH);
            File propsFile = new File(filePath);
            tool = tool.mergeWith(ParameterTool.fromPropertiesFile(propsFile));
        }

        return tool.getProperties();
    }

}
