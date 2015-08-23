package com.springdeveloper.demo.cloud;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

@SpringBootApplication
public class HdfsWriterApplication {

	public static void main(String[] args) throws InterruptedException {
		SpringApplication.run(HdfsWriterApplication.class, args);
	}

}
