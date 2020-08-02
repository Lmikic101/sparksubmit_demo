#! /bin/bash

if [ $# -eq 0 ]
then
    DATE=$(date -d"1 day ago" "+%Y%m%d")
    HOUR=$(date -d"1 hour ago" "+%H")
    PROCESS="all"
elif [ $# -eq 1 ]
then
    DATE=$1
    HOUR=$(date -d"1 hour ago" "+%H")
    PROCESS="all"
elif [ $# -eq 2 ]
then
    DATE=$1
    HOUR=$2
    PROCESS="all"
else
    DATE=$1
    HOUR=$2
    PROCESS=$3
fi

function init()
{
    SPART_SUBMIT="/usr/bin/spark-submit"
    HADOOP_CLIENT="/usr/bin/hadoop"
    PROCESS_NAME="sparksubmit-0.0.1-SNAPSHOT.jar"
    JAVA_MAIN_CLASS="com.demo.sparksubmit.GetidApplication"
    JOB_NAME="spark-submit-java_${DATE}_lzx"

}

function prepare()
{
    if [ "${PROCESS_NAME}"x == ""x ]
    then
        echo "!!! please input a python file"
        exit 1
    fi
}

function now_time()
{
    now_ts=$(date "+%Y%m%d %H:%M:%S")
    echo "${now_ts}" | awk '{print $0}'
}

function justdoit()
{
${SPART_SUBMIT} \
    --master yarn \
    --deploy-mode client \
    --driver-memory 4G \
    --executor-memory 4G \
    --num-executors 80 \
    --executor-cores 2 \
    --queue adx_online \
    --conf spark.executor.memoryOverhead=2G \
    --conf spark.dynamicAllocation.minExecutors=70 \
    --conf spark.dynamicAllocation.maxExecutors=80 \
    --conf spark.default.parallelism=1600 \
    --conf spark.sql.shuffle.partitions=1600 \
    --conf spark.speculation=true \
    --files application.yml \
    "${PROCESS_NAME}" "${JOB_NAME}" \
                      "${DATE}" \
                      "${HOUR}" \
                      "${PROCESS}"

if [ $? -ne 0 ]
then
    echo "!!! Process[${JOB_NAME}:${DATE}] fail, please check"
    exit 1
else
    now_ts=$(now_time)
    echo "=== [${now_ts}] Mission success[${JOB_NAME}:${DATE}"
fi
}

function main()
{
    init
    prepare
    justdoit
}

main

