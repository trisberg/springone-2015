package com.springdeveloper.demo;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class IngestApplication implements CommandLineRunner {

	@Autowired
	private IngestProcessor ingestProcessor;
	
    public static void main(String[] args) {
        SpringApplication.run(IngestApplication.class, args);
    }

	@Override
	public void run(String... arg0) throws Exception {
        System.out.println("*** RUNNING ...");
		ingestProcessor.process();
	}
}
