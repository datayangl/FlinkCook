package com.code.ly.flink.dev.formats.text;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

public class TextOptions {
    public static final ConfigOption<Boolean> FAIL_ON_MISSING_FIELD = ConfigOptions.key("fail-on-missing-field").booleanType().defaultValue(false).withDescription("Optional flag to specify whether to fail if a field is missing or not, false by default.");
    public static final ConfigOption<Boolean> IGNORE_PARSE_ERRORS = ConfigOptions.key("ignore-parse-errors").booleanType().defaultValue(false).withDescription("Optional flag to skip fields and rows with parse errors instead of failing;\nfields are set to null in case of errors, false by default.");
    public static final ConfigOption<String> FIELD_INDEX = ConfigOptions.key("field-index").stringType().defaultValue(null).withDescription("Optional config to describe index of fields when text is split into string array ; null by default.");
    public static final ConfigOption<String> FIELD_SPLIT= ConfigOptions.key("field-split").stringType().defaultValue(",").withDescription("Optional config to describe split word of text; ',' by default.");
    public static final ConfigOption<String> TIMESTAMP_FORMAT = ConfigOptions
            .key("timestamp-format.standard")
            .stringType()
            .defaultValue("SQL")
            .withDescription("Optional flag to specify timestamp format, SQL by default." +
                    " Option ISO-8601 will parse input timestamp in \"yyyy-MM-ddTHH:mm:ss.s{precision}\" format and output timestamp in the same format." +
                    " Option SQL will parse input timestamp in \"yyyy-MM-dd HH:mm:ss.s{precision}\" format and output timestamp in the same format.");

}
