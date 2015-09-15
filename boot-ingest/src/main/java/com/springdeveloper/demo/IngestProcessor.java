package com.springdeveloper.demo;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.hadoop.store.DataStoreWriter;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

@Component
public class IngestProcessor {
	
	@Value("${tweets.fileName}")
	private String fileName;

	@Autowired
	DataStoreWriter<String> writer;

	private final ObjectMapper jsonMapper = new ObjectMapper(new JsonFactory());

	public void process() {
        try(BufferedReader br = new BufferedReader(new FileReader(fileName))) {
            String line = br.readLine();
            while (line != null) {
            	writer.write(processLine(line));
                line = br.readLine();
            }
			writer.close();
		} catch (IOException e) {
			System.out.println("Error processing file: " + e.getMessage());
		}
	}
	
	private String processLine(String line) throws IOException {
		Map<String, Object> tweet = 
				jsonMapper.readValue(line, new TypeReference<HashMap<String,  Object>>(){});
		@SuppressWarnings("unchecked")
		Map<String, Object> user = (Map<String, Object>) tweet.get("user");
		StringBuilder csvData = new StringBuilder();
		csvData.append(tweet.get("id"));
		csvData.append("," + user.get("screen_name"));
		csvData.append("," + tweet.get("created_at"));
		csvData.append("," + tweet.get("text").toString().replace(",", "\\,").replace('\n', ' '));
		csvData.append("," + user.get("followers_count"));
		return csvData.toString();
	}
}
