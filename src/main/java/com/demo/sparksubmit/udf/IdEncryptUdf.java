package com.demo.sparksubmit.udf;

import com.demo.sparksubmit.util.IdEncryptUtil;
import org.apache.spark.sql.api.java.UDF1;

/**
 * @Created by IntelliJ IDEA.
 * @author: MikeLiu
 * @Date: 2020/7/29
 * @Time: 23:59
 * @des:
 */
public class IdEncryptUdf {

    public static UDF1<String, String> idEncoderUdf = new UDF1<String, String>() {
        @Override
        public String call(String s) throws Exception {
            return IdEncryptUtil.encrypt(s);
        }
    };

    public static UDF1<String, String> idDecoderUdf = new UDF1<String, String>() {
        @Override
        public String call(String s) throws Exception {
            return IdEncryptUtil.decrypt(s);
        }
    };

}
