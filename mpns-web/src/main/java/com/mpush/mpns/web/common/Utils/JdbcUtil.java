package com.mpush.mpns.web.common.Utils;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;

/**
 * @author <a href="mailto:javaworld@qq.com">Woody</a> on 2014-07-02
 * @since 1.0
 */
public class JdbcUtil {
    /**
     * @param txt
     * @return 对html中的特殊字符进行编码
     */
    public static String HTMLEncode(String txt) {
        txt = txt.replaceAll("&", "&amp;");
        txt = txt.replaceAll("<", "&lt;");
        txt = txt.replaceAll(">", "&gt;");
        txt = txt.replaceAll("\"", "&quot;");
        txt = txt.replaceAll("'", "&apos;");
        return txt;
    }

    /**
     * 对入库字符串进行加引号处理，并且对单引号和右斜杠进行转义，防止SQL注入
     * 入库字串特别需要保留Html标签时使用此方法
     */
    public static String getHtmlStringValue(Object obj) {
        if (obj == null)
            return null;
        else
            return  obj.toString().replace("'", "''").replace("\\", "\\\\") ;
    }


    /**
     * 对入库字符串进行加引号处理，并且对Html和右斜杠进行转义，防止HTML注入以及SQL注入
     */
    public static String getStringValue(Object obj) {
        if (obj == null)
            return null;
        else
            return  getBareStringValue(obj) ;
    }

    public static String getBareStringValue(Object obj) {
        if (obj == null)
            return null;
        else
            return HTMLEncode(obj.toString()).replace("\\", "\\\\");
    }


    /**
     * @return 判断传入的参数值，如果为空，则返回null，否则返回对象的值,不带单引号,如："123"
     */
    public static String getNumberValue(Object obj) {
        if (obj == null || "".equals(obj.toString()))
            return null;
        else
            return obj.toString();
    }

    /**
     * @return 将List转为SQL中适用于"IN"关键字的串
     * by zaweze on 2016-03-04
     */
    public static String getInListValue(List<? extends Object> list) {
        StringBuilder result = new StringBuilder("(");
        if (list.get(0) instanceof Integer) {
            list.stream().map(JdbcUtil::getNumberValue).forEach(x -> result.append(x).append(","));
        } else {
            list.stream().map(JdbcUtil::getStringValue).forEach(x -> result.append(x).append(","));
        }
        return result.toString().substring(0, result.length() - 1) + ")";
    }


    /**
     * @return 将逗号分隔的String转为SQL中适用于"IN"关键字的串
     * by zzl on 2016-07-22
     */
    public static String getInStringValue(String inString) {
        List<String> inlist = Arrays.asList(inString.split(","));

        if (inlist.size() == 0) {
            return "('')";
        }
        return getInListValue(inlist);
    }



    /**
     * 传入s格式为 yyyy-MM-dd HH:mm:ss
     *
     * @return 返回整型日期
     */
    public static String getUnixDate(String s) {
        if (s == null || "".equals(s))
            return null;
        return "UNIX_TIMESTAMP(" + getStringValue(s) + ")";
    }


    /**
     * @return 带引号的格式化的时间到秒
     */
    public static String getLocalDateTime(LocalDateTime dateTime){
        if (dateTime == null)
            return null;
        return dateTime.format(DateTimeFormatter.ISO_LOCAL_DATE)+" "+ dateTime.format(DateTimeFormatter.ISO_LOCAL_TIME);
    }

}
