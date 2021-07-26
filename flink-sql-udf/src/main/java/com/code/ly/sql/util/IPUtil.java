package com.code.ly.sql.util;

public class IPUtil {
    public static boolean isValidIpv6(String str) {
        /**
         * 备注：fe80 是局域网ip的前缀，丢弃
         * **/
        return StringUtil.isNotBlank(str) && str.split(":").length > 3 && !str.toLowerCase().startsWith("fe80") ;
    }
}
