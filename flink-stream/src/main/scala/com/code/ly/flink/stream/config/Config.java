package com.code.ly.flink.stream.config;

import java.util.Properties;

/**
 * @author luoy
 */
public interface Config {
    /**
     * 返回配置的组件，比如kafka
     * @return
     */
    String getComponent();
}
