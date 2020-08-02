package com.demo.sparksubmit.entity;

import lombok.Getter;
import lombok.Setter;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.List;

/**
 * @Created by IntelliJ IDEA.
 * @author: MikeLiu
 * @Date: 2020/7/30
 * @Time: 14:56
 * @des:
 */
@Setter
@Getter
public class JobBaseParamEntity {

    private String jobName;

    private List<String> inputPath ;

    private List<String> outputPath;

    private SparkSession spark;

    public JobBaseParamEntity() {
        this.inputPath = new ArrayList<>();
        this.outputPath = new ArrayList<>();
    }
}
