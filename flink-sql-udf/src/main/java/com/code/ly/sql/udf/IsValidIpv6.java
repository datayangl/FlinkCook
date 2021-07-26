package com.code.ly.sql.udf;

import com.code.ly.sql.util.IPUtil;
import org.apache.flink.table.functions.ScalarFunction;

public class IsValidIpv6 extends ScalarFunction {
    public boolean eval(String ipv6) {
        return IPUtil.isValidIpv6(ipv6);
    }
}
