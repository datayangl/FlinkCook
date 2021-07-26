package com.code.ly.sql.formats.csv2;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

public class CsvUdfOptions {
    public static final ConfigOption<String> SECOND_FIELD_DELIMITER = ConfigOptions.key("second-field-delimiter").stringType().defaultValue(",").withDescription("");
    public static final ConfigOption<String> DISCARD_PREFIX_FIELD_DELIMITER = ConfigOptions.key("discard-prefix-field-delimiter").stringType().defaultValue("").withDescription("");
    public static final ConfigOption<Boolean> IGNORE_TRAILING_UNMAPPABLE = ConfigOptions.key("ignore-trailing-unmappable").booleanType().defaultValue(false).withDescription("");
}
