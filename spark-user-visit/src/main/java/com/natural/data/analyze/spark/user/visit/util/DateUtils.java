package com.natural.data.analyze.spark.user.visit.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class DateUtils {

    public static final SimpleDateFormat TIME_FORMAT =
            new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    public static final SimpleDateFormat DATE_FORMAT =
            new SimpleDateFormat("yyyy-MM-dd");
    public static final SimpleDateFormat DATEKEY_FORMAT =
            new SimpleDateFormat("yyyyMMdd");

    public static Date parseTime(String time) {
        try {
            return TIME_FORMAT.parse(time);
        } catch (ParseException e) {
            e.printStackTrace();
        } catch (NumberFormatException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     *
     * @param datetime    2020-05-07 04:02:07   yyyy-MM-dd HH:mm:ss
     * @return            2020-05-07_04
     */
    public static String getDateHour(String datetime) {
        String[] d = datetime.split(" ");
        String date = d[0];
        String hourMinuteSecond = d[1];
        String hour = hourMinuteSecond.split(":")[0];
        return date + "_" + hour;
    }

    public static String formatTime(Date date) {
        return TIME_FORMAT.format(date);
    }



}
