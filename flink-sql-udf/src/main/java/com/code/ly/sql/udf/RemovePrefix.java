package com.code.ly.sql.udf;

import org.apache.flink.table.functions.ScalarFunction;

public class RemovePrefix extends ScalarFunction {
    public String eval(String str, String prefix) {
        if (str.startsWith(prefix)){
            return str.substring(prefix.length());
        }

        return str;
    }
}
