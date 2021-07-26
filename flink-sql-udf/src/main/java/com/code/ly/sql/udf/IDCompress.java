package com.code.ly.sql.udf;

import com.getui.axe.v3.data.utils.IdCompressUtil;
import org.apache.flink.table.functions.ScalarFunction;

public class IDCompress extends ScalarFunction {
    public String eval(String gid) {
        String result = null;
        try {
            result = IdCompressUtil.getGidValue(gid.getBytes());
        } catch (Exception e) {

        }
        return result;
    }
}
