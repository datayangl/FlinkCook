package com.code.ly.sql.udf;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

@FunctionHint(output = @DataTypeHint("ROW<s STRING>"))
public class ExplodeSplit extends TableFunction<Row> {
    private String separator = ",";

    public void eval(String str) {
        for (String s:str.split(separator, -1)) {
            collect(Row.of(s));
        }
    }

    public void eval(String str, String separator) {
        for (String s:str.split(separator, -1)) {
            collect(Row.of(s));
        }
    }
}
