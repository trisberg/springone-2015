package com.springdeveloper.demo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.apache.hive.jdbc.HiveDriver;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.configuration.annotation.BatchConfigurer;
import org.springframework.batch.core.configuration.annotation.DefaultBatchConfigurer;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.MultiResourceItemReader;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.jdbc.DataSourceBuilder;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Primary;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.data.hadoop.batch.hive.HiveTasklet;
import org.springframework.data.hadoop.fs.HdfsResourceLoader;
import org.springframework.data.hadoop.hive.HiveClientFactory;
import org.springframework.data.hadoop.hive.HiveClientFactoryBean;
import org.springframework.data.hadoop.hive.HiveScript;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.SimpleDriverDataSource;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseBuilder;
import org.springframework.jdbc.datasource.init.DataSourceInitializer;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;

@Configuration
public class BatchHive2Configuration {
	
	@Value("${tweets.hiveUrl}")
	private String hiveUrl;

	@Autowired
    private StepBuilderFactory steps;
	
	@Bean
	Job tweetInfluencers(JobBuilderFactory jobs, Step hiveInfluencers, Step exportInfluencers, Step results) throws Exception {
	    return jobs.get("TweetInfluencers")
	    		.start(hiveInfluencers)
	    		.next(exportInfluencers)
	    		.next(results)
	    		.build();
	}
	 
	@Bean
    Step hiveInfluencers(Tasklet hiveInfluencersTasklet) throws Exception {
		return steps.get("hiveInfluencers")
    		.tasklet(hiveInfluencersTasklet)
            .build();
    }

	@Bean
    Step exportInfluencers(ItemReader<Map<String, Object>> hdfsFileReader, ItemWriter<Map<String, Object>> jdbcWriter) throws Exception {
		return steps.get("exportInfluencers")
    		.<Map<String, Object>, Map<String, Object>> chunk(100)
    		.reader(hdfsFileReader)
    		.writer(jdbcWriter)
            .build();
    }
	
	@Bean
    Step results(Tasklet resultsTasklet) throws Exception {
		return steps.get("results")
    		.tasklet(resultsTasklet)
            .build();
    }

	@Bean
	Tasklet hiveInfluencersTasklet(HiveClientFactory hiveClientFactory) throws Exception {
		HiveTasklet hiveTasklet = new HiveTasklet();
		hiveTasklet.setHiveClientFactory(hiveClientFactory);
		hiveTasklet.setScripts(hiveScripts());
		return hiveTasklet;
	}

	@Bean
	Tasklet resultsTasklet(@Qualifier("exportDataSource") final DataSource exportDataSource) {
		return new Tasklet() {
			@Override
			public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
				JdbcTemplate jdbcTemplate = new JdbcTemplate(exportDataSource);
				System.out.println("Results:");
				List<Map<String, Object>> results = jdbcTemplate.queryForList("select * from twitter_influencers");
				for (Map<String, Object> r : results) {
					System.out.println(" " + r);
				}
				return RepeatStatus.FINISHED;
			}
		};
	}
	
	@Bean
	Collection<HiveScript> hiveScripts() {
		List<HiveScript> hiveScripts = new ArrayList<>();
		hiveScripts.add(new HiveScript(new ClassPathResource("initTweets.hql")));
		hiveScripts.add(new HiveScript(new ClassPathResource("topInfluencers.hql")));
		return hiveScripts;
	}

	@Bean
	@StepScope
	ItemReader<Map<String, Object>> hdfsFileReader(HdfsResourceLoader resourceLoader, LineMapper<Map<String, Object>> lineMapper) throws IOException {
		MultiResourceItemReader<Map<String, Object>> multiReader = new MultiResourceItemReader<>();
		Resource[] resources = resourceLoader.getResources("/demo/influencers/*");
		multiReader.setResources(resources);	
		FlatFileItemReader<Map<String, Object>> itemReader = new FlatFileItemReader<>();
		itemReader.setLineMapper(lineMapper);
		multiReader.setDelegate(itemReader);
		return multiReader;
	}

	@Bean
	HdfsResourceLoader hdfsResourceLoader(org.apache.hadoop.conf.Configuration hadoopConfiguration) {
		return new HdfsResourceLoader(hadoopConfiguration);
	}
	
	@Bean 
	LineMapper<Map<String, Object>> lineMapper() {
		return new LineMapper<Map<String, Object>>() {
			@Override
			public Map<String, Object> mapLine(String line, int lineNum) throws Exception {
				String[] tokens = line.split("\u0001");
				if (tokens.length != 2) {
					throw new DataIntegrityViolationException("Expecting 2 tokens in input line: " + line);
				}
				Map<String, Object> data = new HashMap<String, Object>();
				data.put("user_name", tokens[0]);
				data.put("followers", tokens[1]);
				return data;
			}			
		};
	}
	
	@Bean
	ItemWriter<Map<String, Object>> jdbcWriter(@Qualifier("exportDataSource") DataSource exportDataSource) {
		JdbcBatchItemWriter<Map<String, Object>> writer = new JdbcBatchItemWriter<>();
		writer.setDataSource(exportDataSource);
		writer.setSql("INSERT INTO twitter_influencers (user_name, followers) VALUES (:user_name, :followers)");
		return writer;
	}

	@Bean
	DataSource hiveDataSource() {
		return new SimpleDriverDataSource(new HiveDriver(), hiveUrl);
	}
	
	@Bean
	HiveClientFactory hiveClientFactory(@Qualifier("hiveDataSource") DataSource hiveDataSource) throws Exception {
		HiveClientFactoryBean hiveClientFactoryBean = new HiveClientFactoryBean();
		hiveClientFactoryBean.setHiveDataSource(hiveDataSource);
		hiveClientFactoryBean.afterPropertiesSet();
		return hiveClientFactoryBean.getObject();
	}

	@Bean
	BatchConfigurer batchConfigurer(DataSource dataSource) {
		return new DefaultBatchConfigurer(dataSource);
	}
	
	@Bean
	@Primary
	DataSource batchDataSource() {
		return new EmbeddedDatabaseBuilder()
				.setName("jobs")
				.build();
	}

	@Bean
	public DataSourceInitializer exportDataSourceInitializer(@Qualifier("exportDataSource") DataSource exportDataSource) {
		ResourceDatabasePopulator resourceDatabasePopulator = new ResourceDatabasePopulator();
		resourceDatabasePopulator.addScript(new ClassPathResource("influencers-schema.sql"));
		DataSourceInitializer dataSourceInitializer = new DataSourceInitializer();
		dataSourceInitializer.setDataSource(exportDataSource);
		dataSourceInitializer.setDatabasePopulator(resourceDatabasePopulator);
		return dataSourceInitializer;
	}

    @Bean
	@ConfigurationProperties(prefix="export.jdbc")
	public DataSource exportDataSource() {
	    return DataSourceBuilder.create().build();
	}
	
}
