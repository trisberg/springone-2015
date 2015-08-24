package com.springdeveloper.demo;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@EnableBatchProcessing
public class BatchHive2Application implements CommandLineRunner {

	@Autowired
	JobLauncher jobLauncher;
	
	@Autowired
	Job tweetInfluencers;
	
    public static void main(String[] args) {
        SpringApplication.run(BatchHive2Application.class, args);
    }

	@Override
	public void run(String... args) throws Exception {
		System.out.println("RUNNING ...");
		jobLauncher.run(tweetInfluencers, new JobParametersBuilder().toJobParameters());
	}
    
}
