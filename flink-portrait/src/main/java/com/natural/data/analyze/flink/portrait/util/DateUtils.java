package com.natural.data.analyze.flink.portrait.util;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.Calendar;
import java.util.Date;

public class DateUtils {

    public static int getDaysBetweenByStartAndEnd(String startTime, String endTime, String dateFormatTemple) throws ParseException {
//        DateFormat dateFormat = new SimpleDateFormat(dateFormatTemple);
//        Date start = dateFormat.parse(startTime);
//        Date end = dateFormat.parse(endTime);
//
//        Calendar startCalendar = Calendar.getInstance();
//        Calendar endCalendar = Calendar.getInstance();

        LocalTime start = LocalTime.parse(startTime, DateTimeFormatter.ofPattern(dateFormatTemple));
        LocalTime end = LocalTime.parse(endTime, DateTimeFormatter.ofPattern(dateFormatTemple));
        Duration between = Duration.between(start, end);
        // 精确的时间
        return  (int)between.toDays();
    }
}
