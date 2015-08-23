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

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;

import javax.validation.constraints.Pattern;

/**
 * Used to configure the Hdfs Writer module options that are related to writing to Hdfs.
 *
 * @author Thomas Risberg
 */
@ConfigurationProperties
public class HdfsWriterProperties {

	/**
	 * base path to write files to
	 */
	private String directory = "/demo/test";

	/**
	 * the base filename to use for the created files
	 */
	@Value("${spring.application.name:data}")
	private String fileName;

	/**
	 * the base filename extension to use for the created files
	 */
	private String fileExtension = "txt";

	/**
	 * compression codec alias name (gzip, snappy, bzip2, lzo, or slzo)
	 */
	private String codec = null;

	/**
	 * threshold in bytes when file will be automatically rolled over.
	 */
	private int rollover = 1000000000;

	/**
	 * inactivity timeout in ms after which file will be automatically closed
	 */
	private long idleTimeout = 0L;

	/**
	 * timeout in ms, regardless of activity, after which file will be automatically closed
	 */
	private long closeTimeout = 0L;

	public String getDirectory() {
		return directory;
	}

	public void setDirectory(String directory) {
		this.directory = directory;
	}

	public String getFileName() {
		return fileName;
	}

	public void setFileName(String fileName) {
		this.fileName = fileName;
	}

	public String getFileExtension() {
		return fileExtension;
	}

	public void setFileExtension(String fileExtension) {
		this.fileExtension = fileExtension;
	}

	public int getRollover() {
		return rollover;
	}

	public void setRollover(int rollover) {
		this.rollover = rollover;
	}

	public long getIdleTimeout() {
		return idleTimeout;
	}

	public void setIdleTimeout(long idleTimeout) {
		this.idleTimeout = idleTimeout;
	}

	public long getCloseTimeout() {
		return closeTimeout;
	}

	public void setCloseTimeout(long closeTimeout) {
		this.closeTimeout = closeTimeout;
	}

}
