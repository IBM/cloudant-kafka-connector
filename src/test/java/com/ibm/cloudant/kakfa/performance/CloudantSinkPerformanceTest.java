package com.ibm.cloudant.kakfa.performance;

import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

import com.carrotsearch.junitbenchmarks.AbstractBenchmark;
import com.carrotsearch.junitbenchmarks.BenchmarkOptions;
import com.cloudant.client.api.Database;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.ibm.cloudant.kafka.common.InterfaceConst;
import com.ibm.cloudant.kafka.common.utils.JavaCloudantUtil;
import com.ibm.cloudant.kafka.connect.CloudantSinkTask;
import com.ibm.cloudant.kakfa.connect.utils.CloudantDbUtils;
import com.ibm.cloudant.kakfa.connect.utils.ConnectorUtils;
 
public class CloudantSinkPerformanceTest extends AbstractBenchmark {	
	private static Database targetDb;	
	private static JsonObject testResults1 = new JsonObject();
	private static JsonObject testResults2 = new JsonObject();
	private static JsonObject testResults3 = new JsonObject();
		
	private Properties defaultProperties;	
	private Map<String, String> targetProperties;  
	private CloudantSinkTask sinkTask;
	private List<SinkRecord> sinkRecords;
				
	@Before
	public void setUp() throws Exception {	
		//Set properties
		defaultProperties = new Properties();
		defaultProperties.load(new FileReader(new File("src/test/resources/test.properties")));
		targetProperties = ConnectorUtils.getTargetProperties(defaultProperties);
		
		// Create a _target database to replicate data into	
		targetDb = JavaCloudantUtil.getDBInst(defaultProperties.getProperty("performance.url") + "_target", 
				defaultProperties.get(InterfaceConst.USER_NAME).toString(),
				defaultProperties.get(InterfaceConst.PASSWORD).toString());
		
		//Create Connector
		sinkTask = new CloudantSinkTask();		
		
		//Create Kafka SinkRecord
		sinkRecords = new ArrayList<SinkRecord>();
	}
	
	@BenchmarkOptions(benchmarkRounds = 3, warmupRounds = 0)
	@Test
	public void testSinkPerformance() throws Exception {
		//set parameter => init(docsNumber, topic, batch.size, tasks.max, guid.schema)
		init(100000, 1, 10000, 1, false);
		long testTime = runTest();
		testResults1 = addResults(testResults1, testTime);
	}
	
	@BenchmarkOptions(benchmarkRounds = 3, warmupRounds = 0)
	@Test
	public void testSinkPerformance2() throws Exception {
		//set parameter => init(docsNumber, topic, batch.size, tasks.max, guid.schema)
		init(100000, 1, 10000, 1, false);
		long testTime = runTest();
		testResults2 = addResults(testResults2, testTime);
	}
	
	@BenchmarkOptions(benchmarkRounds = 3, warmupRounds = 0)
	@Test
	public void testSinkPerformance3() throws Exception {
		//set parameter => init(docsNumber, topic, batch.size, tasks.max, guid.schema)
		init(100000, 1, 10000, 1, false);
		long testTime = runTest();
		testResults3 = addResults(testResults3, testTime);
	}
	
	public void init(int numberDocs, int numTopics, int batch_size, int tasks_max, Boolean replication) {				
		List<String> topics = new ArrayList<String>();
		JsonParser parser = new JsonParser();
		
		for(int i=0; i<numTopics; i++) {
			topics.add("topic" + i);
		}
		
		for (String topic : topics) {
			for(int i=0; i<numberDocs; i++) {		
				JsonObject doc = parser.parse("{\"_id\":\"" + i + "\"}").getAsJsonObject();
						
				sinkRecords.add(new SinkRecord(topic, 0, null, null, Schema.STRING_SCHEMA, doc, i));	
			}		
		}
		targetProperties.put(InterfaceConst.URL, defaultProperties.getProperty("performance.url") + "_target");
		targetProperties.put(InterfaceConst.TOPIC, topics.toString());
		targetProperties.put(InterfaceConst.BATCH_SIZE, Integer.toString(batch_size));
		targetProperties.put(InterfaceConst.TASKS_MAX, Integer.toString(tasks_max));
		targetProperties.put(InterfaceConst.REPLICATION, replication.toString());
	}
	
	public JsonObject addResults(JsonObject results, long testTime) {
		if(results.size() == 0){			
			JsonArray testTimes = new JsonArray();
			testTimes.add(testTime);			
			results.addProperty("testRounds", 1);
			results.addProperty("diskSize", targetDb.info().getDiskSize());
			results.addProperty("documents", targetDb.info().getDocCount());
			
			results.addProperty(InterfaceConst.TOPIC, targetProperties.get(InterfaceConst.TOPIC));
			results.addProperty(InterfaceConst.BATCH_SIZE, targetProperties.get(InterfaceConst.BATCH_SIZE));
			results.addProperty(InterfaceConst.TASKS_MAX, targetProperties.get(InterfaceConst.TASKS_MAX));
			results.addProperty(InterfaceConst.REPLICATION, targetProperties.get(InterfaceConst.REPLICATION));
			results.add("testTimes", testTimes);	
		}
		else {
			results.addProperty("testRounds", results.get("testRounds").getAsInt() + 1);
			results.get("testTimes").getAsJsonArray().add(testTime);
		}
		return results;
	}
	
	public long runTest() throws Exception {									
		// KAFKA
		HashMap<TopicPartition, OffsetAndMetadata> offsets = new HashMap<TopicPartition, OffsetAndMetadata>();		
		
		// 1. Create Connector and Trigger sourceTask to get a batch of records		
		sinkTask.start(targetProperties);			
			
		// 2. Measure SourceRecords
		long startTime = System.currentTimeMillis();				
		
		sinkTask.put(sinkRecords);
		sinkTask.flush(offsets);
		
		long endTime = System.currentTimeMillis();
		
		//3. Stop sourceTask		
		sinkTask.stop();
		
		return endTime - startTime;
	}
				
	@After
	public void tearDown() throws Exception { 	
		CloudantDbUtils.dropDatabase(
				defaultProperties.getProperty("performance.url") + "_target", 
				defaultProperties.get(InterfaceConst.USER_NAME).toString(), 
				defaultProperties.get(InterfaceConst.PASSWORD).toString());
	}
	
	@AfterClass
	public static void Results() {		
		//Show Properties and BenchmarkRounds
		System.out.println("\n### Results - testSinkPerformance ###");
		ConnectorUtils.showPerformanceResults(testResults1);
		System.out.println("\n### Results - testSinkPerformance2 ###");
		ConnectorUtils.showPerformanceResults(testResults2);
		System.out.println("\n### Results - testSinkPerformance3 ###");
		ConnectorUtils.showPerformanceResults(testResults3);
	}
}
