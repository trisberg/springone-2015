package com.springdeveloper.demo;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.hive.jdbc.HiveDriver;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.hadoop.batch.hive.HiveTasklet;
import org.springframework.data.hadoop.hive.HiveClientFactory;
import org.springframework.data.hadoop.hive.HiveClientFactoryBean;
import org.springframework.data.hadoop.hive.HiveScript;
import org.springframework.jdbc.datasource.SimpleDriverDataSource;

@Configuration
public class BatchHive2Configuration {
	
	@Value("${tweets.hiveUrl}")
	private String hiveUrl;

	@Autowired
    private StepBuilderFactory steps;
	
	@Bean
	public Job tweetInfluencers(JobBuilderFactory jobs) throws Exception {
	    return jobs.get("TweetInfluencers").start(hiveInfluencers()).build();
	}
	 
	@Bean
    public Step hiveInfluencers() throws Exception {
		return steps.get("hiveInfluencers")
    		.tasklet(hiveInfluencersTasklet())
            .build();
    }

	@Bean
	public HiveTasklet hiveInfluencersTasklet() throws Exception {
		HiveTasklet hiveTasklet = new HiveTasklet();
		hiveTasklet.setHiveClientFactory(hiveClientFactory());
		hiveTasklet.setScripts(hiveScripts());
		return hiveTasklet;
	}

	@Bean
	Collection<HiveScript> hiveScripts() {
		List<HiveScript> hiveScripts = new ArrayList<>();
		hiveScripts.add(new HiveScript(new ClassPathResource("initTweets.hql")));
		hiveScripts.add(new HiveScript(new ClassPathResource("topInfluencers.hql")));
		return hiveScripts;
	}
	
	@Bean
	HiveClientFactory hiveClientFactory() throws Exception {
		HiveClientFactoryBean hiveClientFactoryBean = new HiveClientFactoryBean();
		hiveClientFactoryBean.setHiveDataSource(new SimpleDriverDataSource(new HiveDriver(), hiveUrl));
		hiveClientFactoryBean.afterPropertiesSet();
		return hiveClientFactoryBean.getObject();
	}
}
