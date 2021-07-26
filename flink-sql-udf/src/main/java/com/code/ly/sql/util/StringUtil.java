package com.code.ly.sql.util;

public class StringUtil {
    public static boolean isBlank(String str) {
        return str == null || "".equals(str);
    }
    public static boolean isNotBlank(String str) {
        return !isBlank(str);
    }
    public static boolean isNumeric(String str){
        for (int i = str.length();--i>=0;){
            if (!Character.isDigit(str.charAt(i))){
                return false;
            }
        }
        return true;
    }

}
