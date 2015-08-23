/*
 * Copyright 2015 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.springdeveloper.demo.cloud;

import org.apache.hadoop.fs.Path;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.hadoop.store.DataStoreWriter;
import org.springframework.data.hadoop.store.output.TextFileWriter;
import org.springframework.data.hadoop.store.strategy.naming.ChainedFileNamingStrategy;
import org.springframework.data.hadoop.store.strategy.naming.FileNamingStrategy;
import org.springframework.data.hadoop.store.strategy.naming.StaticFileNamingStrategy;
import org.springframework.data.hadoop.store.strategy.naming.UuidFileNamingStrategy;
import org.springframework.data.hadoop.store.strategy.rollover.RolloverStrategy;
import org.springframework.data.hadoop.store.strategy.rollover.SizeRolloverStrategy;

import java.util.Arrays;

/**
 *
 * @author Thomas Risberg
 */
@Configuration
@EnableConfigurationProperties(com.springdeveloper.demo.cloud.HdfsWriterProperties.class)
public class HdfsWriterConfiguration {

	@Autowired
	private HdfsWriterProperties properties;

	@Bean
	DataStoreWriter<String> dataStoreWriter(org.apache.hadoop.conf.Configuration hadoopConfiguration) {
		TextFileWriter writer = new TextFileWriter(hadoopConfiguration,
				new Path(properties.getDirectory()), null);
		ChainedFileNamingStrategy namingStrategy = new ChainedFileNamingStrategy(
				Arrays.asList(new FileNamingStrategy[]{
						new StaticFileNamingStrategy(properties.getFileName()),
						new UuidFileNamingStrategy(),
						new StaticFileNamingStrategy(properties.getFileExtension(), ".")}));
		writer.setFileNamingStrategy(namingStrategy);
		writer.setIdleTimeout(properties.getIdleTimeout());
		writer.setCloseTimeout(properties.getCloseTimeout());
		RolloverStrategy rolloverStrategy = new SizeRolloverStrategy(properties.getRollover());
		writer.setRolloverStrategy(rolloverStrategy);
		return writer;
	}

}
