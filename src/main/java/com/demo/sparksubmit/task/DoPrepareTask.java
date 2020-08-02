package com.demo.sparksubmit.task;

import com.demo.sparksubmit.common.Constants;
import com.demo.sparksubmit.common.Separator;
import com.demo.sparksubmit.config.SparkParamConfig;
import com.demo.sparksubmit.util.DateUtil;
import com.demo.sparksubmit.entity.JobParamEntity;
import io.netty.util.internal.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * @Created by IntelliJ IDEA.
 * @author: MikeLiu
 * @Date: 2020/7/30
 * @Time: 10:20
 * @des:
 */
@Service
public class DoPrepareTask {

    private static final Logger logger = LoggerFactory.getLogger(DoPrepareTask.class);
    @Autowired
    private SparkParamConfig sparkParamConfig;

    public boolean handler(String[] args, JobParamEntity jobParamEntity) {
        if (args.length < 2) {
            logger.error("not enough param");
            return false;
        }
        // spark job name
        String jobName = args[0];
        // date: today
        String today = args[1];
        // date: yesterday
        String yesterday = DateUtil.manyDaysAgo(today, 1);
        // set job param entity: job name, other user defined param
        jobParamEntity.setJobName(jobName);
        jobParamEntity.setCurDate(DateUtil.manyDaysAgoUnixSecond(today, 0));
        jobParamEntity.setAMonthAgo(DateUtil.manyDaysAgoUnixSecond(today, 30));

        if (!StringUtil.isNullOrEmpty(jobName)) {
            this.sparkParamConfig.setJobname(jobName);
        } else {
            this.sparkParamConfig.setJobname(Constants.DEFAULT_JOB_NAME);
        }
        // input file
        List<String> inputs = this.sparkParamConfig.getFilepath().get(Constants.DAILY_INPUT);
        // add daily input
        for (String input : inputs) {
            jobParamEntity.getInputPath().add(input + Separator.FORWARD_SLASH + today);
        }
        // add total yesterday input
        inputs = this.sparkParamConfig.getFilepath().get(Constants.YESTERDAY_INPUT);
        for (String input : inputs) {
            jobParamEntity.getInputPath().add(input + Separator.FORWARD_SLASH + yesterday);
        }
        // output file
        List<String> outputs = this.sparkParamConfig.getFilepath().get(Constants.OUTPUT);
        for (String output : outputs) {
            jobParamEntity.getOutputPath().add(output + Separator.FORWARD_SLASH + today);
        }
        return true;
    }
}
