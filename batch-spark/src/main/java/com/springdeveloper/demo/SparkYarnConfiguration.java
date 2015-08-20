package com.springdeveloper.demo;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.data.hadoop.batch.scripting.ScriptTasklet;
import org.springframework.data.hadoop.batch.spark.SparkYarnTasklet;
import org.springframework.data.hadoop.scripting.HdfsScriptRunner;
import org.springframework.scripting.ScriptSource;
import org.springframework.scripting.support.StaticScriptSource;

@Configuration
public class SparkYarnConfiguration {

	@Autowired
	private org.apache.hadoop.conf.Configuration hadoopConfiguration;

	@Value("${demo.inputDir}")
	String inputDir;
	
	@Value("${demo.inputFileName}")
	String inputFileName;
	
	@Value("${demo.inputLocalDir}")
	String inputLocalDir;
	
	@Value("${demo.outputDir}")
	String outputDir;
	
	@Bean
	public Job tweetInfluencers(JobBuilderFactory jobs, Step initScript, Step sparkInfluencers) throws Exception {
	    return jobs.get("TweetInfluencers")
	    		.flow(initScript)
	    		.next(sparkInfluencers)
	    		.end()
	    		.build();
	}
	 
	@Bean
    public Step initScript(StepBuilderFactory steps, Tasklet scriptTasklet) throws Exception {
		return steps.get("initScript")
    		.tasklet(scriptTasklet)
            .build();
    }

	@Bean
    public Step sparkInfluencers(StepBuilderFactory steps, Tasklet sparkInfluencersTasklet) throws Exception {
		return steps.get("sparkInfluencers")
    		.tasklet(sparkInfluencersTasklet)
            .build();
    }

	@Bean
	public ScriptTasklet scriptTasklet(HdfsScriptRunner scriptRunner) {
		ScriptTasklet scriptTasklet = new ScriptTasklet();
		scriptTasklet.setScriptCallback(scriptRunner);
		return scriptTasklet;
	}

	@Bean HdfsScriptRunner scriptRunner() {
		StringBuilder scriptContent = new StringBuilder();
		scriptContent.append("indir = '" + inputDir + "';\n");
		scriptContent.append("source = '"+inputLocalDir+"';\n");
		scriptContent.append("file = '"+inputFileName+"';\n");
		scriptContent.append("outdir = '"+outputDir +"';\n");
		scriptContent.append("if (fsh.test(indir)) {\n");
		scriptContent.append("	fsh.rmr(indir);\n");
		scriptContent.append("}\n");
		scriptContent.append("if (fsh.test(outdir)) {\n");
		scriptContent.append("	fsh.rmr(outdir);\n");
		scriptContent.append("}\n");
		scriptContent.append("fsh.copyFromLocal(source+'/'+file, indir+'/'+file);\n");
		ScriptSource script = new StaticScriptSource(scriptContent.toString());
		HdfsScriptRunner scriptRunner = new HdfsScriptRunner();
		scriptRunner.setConfiguration(hadoopConfiguration);
		scriptRunner.setLanguage("javascript");
		scriptRunner.setScriptSource(script);
		return scriptRunner;
	}

	@Bean
	public SparkYarnTasklet sparkInfluencersTasklet() throws Exception {
		SparkYarnTasklet sparkTasklet = new SparkYarnTasklet();
		sparkTasklet.setSparkAssemblyJar(
				"hdfs:///app/spark/spark-assembly-1.4.1-hadoop2.6.0.jar");
		sparkTasklet.setHadoopConfiguration(hadoopConfiguration);
		sparkTasklet.setAppClass("Hashtags");
		String jarFile = System.getProperty("user.dir") + "/app/spark-hashtags_2.10-0.1.0.jar";
		sparkTasklet.setAppJar(jarFile);
		sparkTasklet.setExecutorMemory("1G");
		sparkTasklet.setNumExecutors(1);
		sparkTasklet.setArguments(new String[]{inputDir + "/" + inputFileName, outputDir});
		return sparkTasklet;
	}
}
