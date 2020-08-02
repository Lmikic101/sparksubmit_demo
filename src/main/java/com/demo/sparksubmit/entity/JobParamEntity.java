package com.demo.sparksubmit.entity;

import lombok.Getter;
import lombok.Setter;

/**
 * @Created by IntelliJ IDEA.
 * @author: MikeLiu
 * @Date: 2020/7/30
 * @Time: 10:15
 * @des:
 */
@Setter
@Getter
public class JobParamEntity extends JobBaseParamEntity {
    // the unix second of the end of the current date
    private String curDate;
    // the unix second of the start of a month ago
    private String aMonthAgo;
}
