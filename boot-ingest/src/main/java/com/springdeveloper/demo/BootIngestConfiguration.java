package com.springdeveloper.demo;

import java.util.Arrays;

import org.apache.hadoop.fs.Path;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.hadoop.store.DataStoreWriter;
import org.springframework.data.hadoop.store.output.TextFileWriter;
import org.springframework.data.hadoop.store.strategy.naming.ChainedFileNamingStrategy;
import org.springframework.data.hadoop.store.strategy.naming.FileNamingStrategy;
import org.springframework.data.hadoop.store.strategy.naming.StaticFileNamingStrategy;
import org.springframework.data.hadoop.store.strategy.naming.UuidFileNamingStrategy;

@Configuration
public class BootIngestConfiguration {

	@Value("${tweets.basePath}")
	private String basePath;

	@Bean
	DataStoreWriter<String> dataStoreWriter(org.apache.hadoop.conf.Configuration hadoopConfiguration) {
		TextFileWriter writer = new TextFileWriter(hadoopConfiguration, new Path(basePath), null);
		ChainedFileNamingStrategy namingStrategy = new ChainedFileNamingStrategy(
				Arrays.asList(new FileNamingStrategy[] {
						new StaticFileNamingStrategy("tweets"),
						new UuidFileNamingStrategy(),
						new StaticFileNamingStrategy("dat", ".")}));
		writer.setFileNamingStrategy(namingStrategy);
		return writer;
	}
}
