package com.demo.sparksubmit.common;

public class Constants {
    
    public static final  String ALL_INDUSTRY_KEYWORD_CACHE_FILE = "all_industry_keyword.json";

    public static final String INPUT = "input";

    public static final String DAILY_INPUT = "dailyinput";

    public static final String YESTERDAY_INPUT = "yesinput";

    public static final String OUTPUT = "output";

    public static final String DEFAULT_JOB_NAME = "spark_default_job_name";

    public static final String EMPTY_STRING = "";
    
    private Constants() {
        throw new IllegalStateException("Utility class");
    }
}
