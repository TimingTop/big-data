package com.natural.data.analyze.core.util;

public class StringUtils {

    public static boolean isEmpty(String str) {
        return str == null || "".equals(str);
    }

    public static String fulfill(String str) {
        if (str.length() == 1) {
            return "0" + str;
        }
        return str;
    }
}
