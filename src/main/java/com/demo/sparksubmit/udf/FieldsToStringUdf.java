package com.demo.sparksubmit.udf;

import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.api.java.UDF3;

/**
 * @Created by IntelliJ IDEA.
 * @author: liuzhixuan@sogou-inc.com
 * @Date: 2020/8/2
 * @Time: 11:58
 * @des:
 */
public class FieldsToStringUdf {
    // empty string
    private static final String EMPTY_STRING = "";
    // separator
    private static final String TAB = "\t";

    public static UDF2<Object, Object, String> fields2ToStringUdf = new UDF2<Object, Object, String>() {
        @Override
        public String call(Object o1, Object o2) throws Exception {
            return new StringBuilder()
                .append(objectToString(o1))
                .append(TAB)
                .append(objectToString(o2))
                .toString();
        }
    };

    public static UDF3<Object, Object, Object, String> fields3ToStringUdf = new UDF3<Object, Object, Object, String>() {
        @Override
        public String call(Object o1, Object o2, Object o3) throws Exception {
            return new StringBuilder()
                    .append(objectToString(o1))
                    .append(TAB)
                    .append(objectToString(o2))
                    .append(TAB)
                    .append(objectToString(o3))
                    .toString();
        }
    };

    private static String objectToString(Object obj) {
        if (null == obj) {
            return EMPTY_STRING;
        } else {
            return String.valueOf(obj);
        }
    }
}
