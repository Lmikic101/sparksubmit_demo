package com.demo.sparksubmit.task;

import com.demo.sparksubmit.common.Separator;
import com.demo.sparksubmit.entity.JobParamEntity;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.types.DataTypes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * @Created by IntelliJ IDEA.
 * @author: MikeLiu
 * @Date: 2020/7/20
 * @Time: 15:57
 * @des:
 */
@Service
public class DoProcessTask {

    private static final Logger logger = LoggerFactory.getLogger(DoProcessTask.class);

    public void handler(JobParamEntity jobParamEntity) {
        // read param
        String userDailyInput = jobParamEntity.getInputPath().get(0);
        String userTotalYesterdayInput = jobParamEntity.getInputPath().get(1);
        String userTotalOutput = jobParamEntity.getOutputPath().get(0);
        String userMonthOutput = jobParamEntity.getOutputPath().get(1);
        String aMonthAgo = jobParamEntity.getAMonthAgo();
        String curDate = jobParamEntity.getCurDate();
        SparkSession spark = jobParamEntity.getSpark();

        // process data
        Dataset<Row> userDaily = spark.read()
                .parquet(userDailyInput)
                .select(functions.callUDF("idDecodeUDF", functions.col("userid")).alias("userid"),
                        functions.col("ts").cast(DataTypes.IntegerType).alias("ts"))
                .repartition(6);
        Dataset<Row> userTotal = spark.read().text(userTotalYesterdayInput)
                .select(functions.split(functions.col("value"), Separator.TAB).alias("split_col"))
                .withColumn("userid", functions.col("split_col").getItem(0))
                .withColumn("ts", functions.col("split_col").getItem(1).cast(DataTypes.IntegerType))
                .drop(functions.col("split_col"))
                .union(userDaily)
                .withColumn("rn", functions.row_number().
                        over(Window
                                .partitionBy(functions.col("userid"))
                                .orderBy(functions.col("ts").desc())))
                .filter("rn=1")
                .drop(functions.col("rn"))
                .cache();
        userTotal.withColumn("final",
                    functions.concat_ws(Separator.TAB, functions.col("userid"), functions.col("ts")))
                .select(functions.col("final"))
                .coalesce(1)
                .write().mode(SaveMode.Overwrite).text(userTotalOutput);

        userTotal.filter("ts>" + aMonthAgo + " and ts<=" + curDate)
                .selectExpr("fields2ToStringUDF(userid, ts) as final")
                .coalesce(1)
                .write().mode(SaveMode.Overwrite).text(userMonthOutput);

        logger.info("date: a month ago:[{}]", aMonthAgo);
        logger.info("date: current day:[{}]", curDate);
    }

}
