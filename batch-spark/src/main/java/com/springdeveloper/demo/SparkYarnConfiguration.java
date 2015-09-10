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
	
	@Value("${demo.sparkAssembly}")
	String sparkAssembly;
	
	@Bean
	Job tweetInfluencers(JobBuilderFactory jobs, Step initScript, Step sparkTopHashtags) throws Exception {
	    return jobs.get("TweetTopHashtags")
	    		.start(initScript)
	    		.next(sparkTopHashtags)
	    		.build();
	}
	 
	@Bean
    Step initScript(StepBuilderFactory steps, Tasklet scriptTasklet) throws Exception {
		return steps.get("initScript")
    		.tasklet(scriptTasklet)
            .build();
    }

	@Bean
    Step sparkTopHashtags(StepBuilderFactory steps, Tasklet sparkTopHashtagsTasklet) throws Exception {
		return steps.get("sparkTopHashtags")
    		.tasklet(sparkTopHashtagsTasklet)
            .build();
    }

	@Bean
	ScriptTasklet scriptTasklet(HdfsScriptRunner scriptRunner) {
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
	SparkYarnTasklet sparkTopHashtagsTasklet() throws Exception {
		SparkYarnTasklet sparkTasklet = new SparkYarnTasklet();
		sparkTasklet.setSparkAssemblyJar(sparkAssembly);
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
