package com.demo.sparksubmit;

import com.demo.sparksubmit.entity.JobParamEntity;
import com.demo.sparksubmit.task.DoInitTask;
import com.demo.sparksubmit.task.DoPrepareTask;
import com.demo.sparksubmit.task.DoProcessTask;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.gson.GsonAutoConfiguration;

//(exclude = {GsonAutoConfiguration.class})
@SpringBootApplication(exclude = {GsonAutoConfiguration.class})
public class GetidApplication implements CommandLineRunner  {
	@Autowired
	private DoPrepareTask doPrepareTask;
	@Autowired
	private DoInitTask doInitTask;
	@Autowired
	private DoProcessTask doProcessTask;

	public static void main(String[] args) {
		SpringApplication.run(GetidApplication.class, args);
	}

	@Override
	public void run(String[] args) throws Exception {
		// param entity
		JobParamEntity jobParamEntity = new JobParamEntity();
		// prepare task: make up input file list and output file list
		doPrepareTask.handler(args, jobParamEntity);
		// init task: init spark session
		doInitTask.handler(jobParamEntity);
		// process task: your process code
		doProcessTask.handler(jobParamEntity);
	}
}
