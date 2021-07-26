package com.code.ly.flink.dev.config;

import java.util.Properties;

/**
 * 配置提取接口
 * 用于适配不同的组件的配置参数和校验
 * @author luoy
 */
public interface ConfigExtractor<T> {
    /**
     * @param props
     * @return
     */
    T extract(Properties props);
}
