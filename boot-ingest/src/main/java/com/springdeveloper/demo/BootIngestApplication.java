package com.springdeveloper.demo;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class BootIngestApplication implements CommandLineRunner {

	@Autowired
	private BootIngestProcessor bootIngestProcessor;
	
    public static void main(String[] args) {
        SpringApplication.run(BootIngestApplication.class, args);
    }

	@Override
	public void run(String... arg0) throws Exception {
        System.out.println("*** RUNNING ...");
		bootIngestProcessor.process();
	}
}
