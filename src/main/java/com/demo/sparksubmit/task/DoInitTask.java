package com.demo.sparksubmit.task;

import com.demo.sparksubmit.entity.JobParamEntity;
import com.demo.sparksubmit.udf.FieldsToStringUdf;
import com.demo.sparksubmit.udf.IdEncryptUdf;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.springframework.stereotype.Service;

/**
 * @Created by IntelliJ IDEA.
 * @author: MikeLiu
 * @Date: 2020/7/30
 * @Time: 10:42
 * @des:
 */
@Service
public class DoInitTask {

    public boolean handler(JobParamEntity jobParamEntity) {
        SparkSession spark = SparkSession.builder()
                .appName(jobParamEntity.getJobName())
                .enableHiveSupport()
                .getOrCreate();
        jobParamEntity.setSpark(spark);
        DoInitTask.registerUdf(spark);
        return true;
    }

    /**
     *  register udf here
     * @param spark SparkSession
     */
    private static void registerUdf(SparkSession spark) {
        spark.udf().register("idDecodeUDF", IdEncryptUdf.idDecoderUdf, DataTypes.StringType);
        spark.udf().register("fields2ToStringUDF", FieldsToStringUdf.fields2ToStringUdf, DataTypes.StringType);
        spark.udf().register("fields3ToStringUDF", FieldsToStringUdf.fields3ToStringUdf, DataTypes.StringType);
    }

}
