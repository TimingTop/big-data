package com.natural.data.analyze.core.util;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

public class DateUtils {

    public static final SimpleDateFormat TIME_FORMAT = new SimpleDateFormat("");

    private static ThreadLocal<DateFormat> timeFormatThreadLocal = new ThreadLocal<DateFormat>() {
        @Override
        protected DateFormat initialValue() {
            return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        }
    };

    private static ThreadLocal<DateFormat> dateFormatThreadLocal = new ThreadLocal<DateFormat>() {
        @Override
        protected DateFormat initialValue() {
            return new SimpleDateFormat("yyyy-MM-dd");
        }
    };
    private static ThreadLocal<DateFormat> dateKeyFormatThreadLocal = new ThreadLocal<DateFormat>() {
        @Override
        protected DateFormat initialValue() {
            return new SimpleDateFormat("yyyyMMdd");
        }
    };

    public static String getTodayDate() {
        return dateFormatThreadLocal.get().format(new Date());
    }

    public static String formatDate(Date date) {
        return dateFormatThreadLocal.get().format(date);
    }

    public static String formatTime(Date date) {
        return timeFormatThreadLocal.get().format(date);
    }

    public static String formatDateKey(Date date) {
        return dateKeyFormatThreadLocal.get().format(date);
    }
}
