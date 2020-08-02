package com.demo.sparksubmit.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * @Created by IntelliJ IDEA.
 * @author: MikeLiu
 * @Date: 2020/7/20
 * @Time: 19:32
 * @des:
 */
public class DateUtil {
    private static final Logger logger = LoggerFactory.getLogger(DateUtil.class);

    public static String manyDaysAgo(String day, int nDays) {
        return addAndSubDate(day, 0 - nDays);
    }

    public static String manyDaysAgoUnixSecond(String day, int nDays) {
        if (nDays == 0) {
            day += "235959";
        } else {
            day += "000000";
        }
        try {
            Calendar cal = Calendar.getInstance();
            // pattern
            SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmSS");
            // string to date
            Date dateParse = sdf.parse(day);
            // add or sub date
            cal.setTime(dateParse);
            cal.add(Calendar.DATE, 0 - nDays);
            return String.valueOf(cal.getTime().getTime() / 1000);
        } catch (Exception e) {
            return null;
        }

    }

    public static String manyDays(String day, int nDays) {
        return addAndSubDate(day, nDays);
    }

    private static String addAndSubDate(String day, int nDays) {
        if (day.length() < 8) {
            logger.error("input date is invalid");
            return null;
        }
        try {
            Calendar cal = Calendar.getInstance();
            // pattern
            SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
            // string to date
            Date dateParse = sdf.parse(day);
            // add or sub date
            cal.setTime(dateParse);
            cal.add(Calendar.DATE, nDays);
            // date to string
            return sdf.format(cal.getTime());
        } catch (Exception e) {
            return null;
        }
    }

    public static void main(String... args) {
        System.out.println("out:" + DateUtil.manyDaysAgo("20200720", 1));
        System.out.println("out:" + DateUtil.manyDaysAgoUnixSecond("20200719", 0));

    }
}
