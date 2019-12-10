package com.cherry.calcite.csv.utils;

public class CommonUtils {

   public static String trimOrNull(String str, String suffix) {
        return str.endsWith(suffix) ? str.substring(0, str.length() - suffix.length())
                : null;
    }

   public static String trim(String str, String suffix) {
        String trimmed = trimOrNull(str, suffix);
        return trimmed != null ? trimmed : str;
    }

}
