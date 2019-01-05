package com.globalegrow.ejob.common.util;

import org.apache.commons.lang3.StringUtils;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * <pre>
 *
 *  File: DateUtil.java
 *
 *  Copyright (c) 2016, globalegrow.com All Rights Reserved.
 *
 *  Description:
 *  日期工具类，实现UTC、local的转换/格式化
 *
 *  Revision History
 *
 *  Date：		2016年8月23日
 *  Author：		luoyaogui
 *
 * </pre>
 */
public class DateUtil {
    public static String patten = "yyyy-MM-dd HH:mm:ss";
    public static String simplePatten = "yyyyMMddHHmmss";

    public static String date2Str(Date date) {
        SimpleDateFormat sdf = new SimpleDateFormat(patten);
        return sdf.format(date);
    }

    public static long getExcuteTime(String endTime, String startTime) {
        if (StringUtils.isEmpty(endTime) || StringUtils.isEmpty(startTime)) {
            return 0;
        }
        if (endTime.equals(startTime)) {
            return 0;
        }

        SimpleDateFormat sdf = new SimpleDateFormat(patten);
        long processTime = 0;
        if (!(StringUtils.isNotEmpty(endTime) && StringUtils.isNotEmpty(startTime))) {
            return processTime;
        }
        try {
            java.util.Date end = sdf.parse(endTime);
            java.util.Date start = sdf.parse(startTime);
            processTime = (end.getTime() - start.getTime()) / 1000;
        } catch (ParseException e) {
            LoggerUtil.warn("rpc request occers exception", e);
        }
        if (processTime > 1400000000) {
            processTime = 0;
        }
        return processTime;
    }

    public static long getInterval(Date endTime, Date startTime) {

        return (endTime.getTime() - startTime.getTime()) / 1000;
    }

    public static Date parse(String time) {

        try {
            return new SimpleDateFormat(patten).parse(time);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return new Date();
    }

    public static String getUtc() {
        SimpleDateFormat sdf = new SimpleDateFormat(patten);
        /*------------------------------UTC--------------------比北京时间慢8小时*/
		/*Calendar cal = Calendar.getInstance();
    	long value = cal.getTimeInMillis();       //your long value.
    	int zoneOffset = cal.get(Calendar.ZONE_OFFSET); 
    	int dstOffset = cal.get(Calendar.DST_OFFSET); 
    	cal.setTimeInMillis(value);
    	cal.add(Calendar.MILLISECOND, -(zoneOffset+dstOffset)); //it only takes int int
        return sdf.format(new Date(cal.getTimeInMillis()));*/
        return sdf.format(new Date());
    }

    /**
     * 因此ejob 调度中心已经将时间+8 hour 了，因此传回去的时候应该减个8 小时
     * @param data
     * @return
     */
    public static String local2Utc(Date date) {
        if(null == date)
            return null;
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        /*------------------------------UTC--------------------比北京时间慢8小时*/
        calendar.set(Calendar.HOUR_OF_DAY, calendar.get(Calendar.HOUR_OF_DAY)-8);
        return new SimpleDateFormat(YYYY_MM_DD_HH_MM_SS).format(calendar.getTime());
    }

    public static String utc2Local(String utcTime, String utcTimePatten) {
        if (null == utcTime || utcTime.length() <= 0) {
            return null;
        }
        try {
            SimpleDateFormat utcFormatter = new SimpleDateFormat(utcTimePatten);
            utcFormatter.setTimeZone(TimeZone.getTimeZone("UTC"));
            Date gpsUTCDate = utcFormatter.parse(utcTime);
            SimpleDateFormat localFormatter = new SimpleDateFormat(patten);
            localFormatter.setTimeZone(TimeZone.getDefault());
            return localFormatter.format(gpsUTCDate.getTime());
        } catch (Throwable e) {
            return null;
        }
    }

    public final static String YYYY_MM_DD_HH_MM_SS = "yyyy-MM-dd HH:mm:ss";

    public static String getFormat(Date date, String format) {
        SimpleDateFormat sdf = new SimpleDateFormat(format);

        return sdf.format(date);
    }

    public static String getFormatNow() {

        return getFormat(new Date(), YYYY_MM_DD_HH_MM_SS);
    }

    /**
     * 统一返回北京时间。如果是美国时间，需要在美国时间的基础上+8 hour
     *
     * @param americOrChain true 表示获取的美国时间，false,表示的北京时间
     * @return
     */
    public static Date getChainDate(boolean americOrChain) {
        if (!americOrChain) {
            return new Date();
        }

        //美国时间比北京时间慢8个小时，统一转换成北京时间，因此需要加 8 hour
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(new Date());
        /*------------------------------UTC--------------------比北京时间慢8小时*/
        calendar.set(Calendar.HOUR_OF_DAY, calendar.get(Calendar.HOUR_OF_DAY) + 8);

        return calendar.getTime();
    }

    public static void main(String[] args) {
        //TimeZone tz = TimeZone.getTimeZone("Asia/Shanghai");
        //String time = DateUtil.utc2Local("2016-12-14 14:00:00",DateUtil.patten);
        System.out.println(getChainDate(true));
        System.out.println(getChainDate(false));
        System.out.println(getFormatedDateString(8));
        System.out.println(TimeUnit.HOURS.toSeconds(1));
    }

    /**
     * timeZoneOffset原为int类型，为班加罗尔调整成float类型
     * timeZoneOffset表示时区，如中国一般使用东八区，因此timeZoneOffset就是8
     *
     * @param timeZoneOffset
     * @return
     */
    public static String getFormatedDateString(float timeZoneOffset) {
        if (timeZoneOffset > 13 || timeZoneOffset < -12) {
            timeZoneOffset = 0;
        }

        int newTime = (int) (timeZoneOffset * 60 * 60 * 1000);
        TimeZone timeZone;
        String[] ids = TimeZone.getAvailableIDs(newTime);
        if (ids.length == 0) {
            timeZone = TimeZone.getDefault();
        } else {
            timeZone = new SimpleTimeZone(newTime, ids[0]);
        }

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        sdf.setTimeZone(timeZone);
        return sdf.format(new Date());
    }

    /**
     * 取北京时间
     *
     * @return
     */
    public static String getBeijingTime() {
        return getFormatedDateString(8);
    }

    public static Date now() {
        return new Date();
    }

    public static Date parseByYYYYMMDDPatten(String source) {
        if (StringUtils.isEmpty(source)) {
            return null;
        }
        DateFormat df = new SimpleDateFormat(simplePatten);
        try {
            return df.parse(source);
        } catch (ParseException e) {
            LoggerUtil.error("time paser error");
        }
        return null;
    }

    /**
     * 指定日期比当前时间大多少天
     *
     * @param date
     * @return
     */
    public static boolean moreThanDaysCurrentTime(Date date, int day) {
        Calendar now = Calendar.getInstance();
        Calendar when = Calendar.getInstance();
        when.setTime(date);
        now.add(Calendar.DAY_OF_YEAR, day);
        return now.before(when);
    }

    /**
     * 指定日期小于当前时间多少天
     *
     * @param date
     * @return
     */
    public static boolean lessThanDaysCurrentTime(Date date, int day) {
        Calendar now = Calendar.getInstance();
        Calendar when = Calendar.getInstance();
        when.setTime(date);
        now.add(Calendar.DAY_OF_YEAR, day);
        return now.after(when);
    }

    public static boolean lessThanSecondCurrentTime(Date date, int val) {
        Calendar now = Calendar.getInstance();
        Calendar when = Calendar.getInstance();
        when.setTime(date);
        now.add(Calendar.SECOND, val);
        return now.after(when);
    }

    /**
     * 比较时间的大小
     *
     * @param date
     * @param when
     * @return
     */
    public static boolean compareTo(Date date, Date when) {
        return date.before(when);
    }

    /**
     * 某天的最晚时间
     *
     * @param t
     * @return
     */
    public static String getLatestTime(Date t) {
        Calendar c = new GregorianCalendar();
        c.setTime(t);
        c.set(Calendar.HOUR_OF_DAY, 23);
        c.set(Calendar.MINUTE, 59);
        c.set(Calendar.SECOND, 59);
        return date2Str(c.getTime());
    }

    /**
     * 某天的最早时间
     *
     * @param t
     * @return
     */
    public static String getEarliestTime(Date t) {
        Calendar c = new GregorianCalendar();
        c.setTime(t);
        c.set(Calendar.HOUR_OF_DAY, 0);
        c.set(Calendar.MINUTE, 0);
        c.set(Calendar.SECOND, 0);
        return date2Str(c.getTime());
    }

    /**
     * 休眠多少时间
     *
     * @param timeout
     * @param unit
     */
    public static void sleep(long timeout, TimeUnit unit) {
        timeout = unit.toSeconds(timeout);
        try {
            TimeUnit.SECONDS.sleep(timeout);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    /**
     * get date string from time millis
     *
     * @param timeMillis
     * @return
     */
    public static String date2Str(long timeMillis) {
        Date date = new Date(timeMillis);
        return date2Str(date);
    }
}
