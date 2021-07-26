package com.code.ly.sql.udf;

import org.apache.flink.table.functions.ScalarFunction;

public class SplitExtractField extends ScalarFunction {
    public String eval(String str, String separator, int index) {
        String[] array = str.split(separator, -1);

        if (index < 0 ) {
            throw new IllegalArgumentException("index must be greater than or equals to 0");
        }

        if (index >= array.length) {
            return null;
        }
        return array[index];
    }
}
