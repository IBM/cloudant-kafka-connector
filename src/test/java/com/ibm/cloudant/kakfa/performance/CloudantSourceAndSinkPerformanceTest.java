package com.ibm.cloudant.kakfa.performance;

import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

import com.carrotsearch.junitbenchmarks.AbstractBenchmark;
import com.carrotsearch.junitbenchmarks.BenchmarkOptions;
import com.cloudant.client.api.Database;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.ibm.cloudant.kafka.common.InterfaceConst;
import com.ibm.cloudant.kafka.common.utils.JavaCloudantUtil;
import com.ibm.cloudant.kafka.connect.CloudantSinkTask;
import com.ibm.cloudant.kafka.connect.CloudantSourceTask;
import com.ibm.cloudant.kakfa.connect.utils.CloudantDbUtils;
import com.ibm.cloudant.kakfa.connect.utils.ConnectorUtils;

public class CloudantSourceAndSinkPerformanceTest extends AbstractBenchmark {	
	private static Database sourceDb;
	private static JsonObject testResults1 = new JsonObject();
	private static JsonObject testResults2 = new JsonObject();
	private static JsonObject testResults3 = new JsonObject();
	
	private Properties defaultProperties;
	private Map<String, String> sourceProperties;  	
	private Map<String, String> targetProperties; 
	private CloudantSourceTask sourceTask;	
	private CloudantSinkTask sinkTask;
	
	@Before
	public void setUp() throws Exception {	
		//Set properties
		defaultProperties = new Properties();
		defaultProperties.load(new FileReader(new File("src/test/resources/test.properties")));
		
		sourceProperties = ConnectorUtils.getSourceProperties(defaultProperties);
		sourceProperties.put(InterfaceConst.URL, defaultProperties.getProperty("performance.url"));
		targetProperties = ConnectorUtils.getTargetProperties(defaultProperties);
		
		// Get the source database handle
		sourceDb = JavaCloudantUtil.getDBInst(
				defaultProperties.getProperty("performance.url"), 
				defaultProperties.get(InterfaceConst.USER_NAME).toString(),
				defaultProperties.get(InterfaceConst.PASSWORD).toString());
		
		//Create Sink Connector	
		sinkTask = new CloudantSinkTask();				
	}
	
	@BenchmarkOptions(benchmarkRounds = 5, warmupRounds = 0)
	@Test
	public void testSourceAndSinkPerformance() throws Exception {
		//set parameter => init(topic, batch.size, tasks.max, guid.schema)
		init("topic", 10000, 1, false);
		long testTime = runTest();
		testResults1 = addResults(testResults1, testTime);
	}
	
	@BenchmarkOptions(benchmarkRounds = 5, warmupRounds = 0)
	@Test
	public void testSourceAndSinkPerformance2() throws Exception {
		//set parameter => init(topic, batch.size, tasks.max, guid.schema)
		init("topic2", 10000, 1, false);
		long testTime = runTest();
		testResults2 = addResults(testResults2, testTime);
	}
	
	@BenchmarkOptions(benchmarkRounds = 5, warmupRounds = 0)
	@Test
	public void testSourceAndSinkPerformance3() throws Exception {
		//set parameter => init(topic, batch.size, tasks.max, guid.schema)
		init("topic3", 10000, 1, false);
		long testTime = runTest();
		testResults3 = addResults(testResults3, testTime);
	}
	
	public void init(String topics, int batch_size, int tasks_max, Boolean replication) {
		//set SourceProperties for SourceTasks
		sourceProperties.put(InterfaceConst.URL, defaultProperties.getProperty("performance.url"));
		sourceProperties.put(InterfaceConst.TOPIC, topics);
		sourceProperties.put(InterfaceConst.BATCH_SIZE, Integer.toString(batch_size));
		sourceProperties.put(InterfaceConst.TASKS_MAX, Integer.toString(tasks_max));
		
		//set TargetProperties for SinkTasks
		targetProperties.put(InterfaceConst.URL, defaultProperties.getProperty("performance.url") + "_target");		
		targetProperties.put(InterfaceConst.TOPIC, topics);
		targetProperties.put(InterfaceConst.BATCH_SIZE, Integer.toString(batch_size));
		targetProperties.put(InterfaceConst.TASKS_MAX, Integer.toString(tasks_max));
		targetProperties.put(InterfaceConst.REPLICATION, replication.toString());
	}
	
	public long runTest() throws Exception {										
		// 1. Trigger sourceTask to get a batch of records
		sourceTask = ConnectorUtils.createCloudantSourceConnector(sourceProperties);
		sourceTask.start(sourceProperties);	
		List<SourceRecord> sourceRecords = new ArrayList<SourceRecord>();
		
		// 2. Trigger sinkTask
		sinkTask.start(targetProperties);	
		List<SinkRecord> sinkRecords = new ArrayList<SinkRecord>();
		//HashMap<TopicPartition, OffsetAndMetadata> offsets = new HashMap<TopicPartition, OffsetAndMetadata>();
				
		// 3. SourceAndSinkTask => MeasureTime
		long startTime = System.currentTimeMillis();	
		do {

			// 3a. Get a batch of source records
			sourceRecords = sourceTask.poll();

			// Process every source record into a corresponding sink record
			// - 1 partition
			// - no schema
			// - no offset
			for (SourceRecord record : sourceRecords) {
				SinkRecord sinkRecord = new SinkRecord(
						record.topic(), // topic
						0, // partition
						record.keySchema(), // key schema
						record.key(), // key
						record.valueSchema(), // value schema
						record.value(),  // value
						0); // offset
				sinkRecords.add(sinkRecord);
			}

			// 3b. Put all sinkRecords to the sink task
			sinkTask.put(sinkRecords);
			sinkRecords.clear();

		} while (sourceRecords.size() > 0);
		long endTime = System.currentTimeMillis();
		
		// 4. Flush the latest set of records in case the batch has not been committed
		sinkTask.flush(null);
				
		// 5. Stop source and target task				
		sourceTask.stop();
		sinkTask.stop();
		
		return endTime - startTime;
	}
	
	public JsonObject addResults(JsonObject results, long testTime) {
		if(results.size() == 0){			
			JsonArray testTimes = new JsonArray();
			testTimes.add(testTime);								
			results.addProperty("testRounds", 1);
			results.addProperty("diskSize", sourceDb.info().getDiskSize());
			results.addProperty("documents", sourceDb.info().getDocCount());
			
			//SourceProperties and TargetProperties should be equal
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
		System.out.println("\n### Results - testSourceAndSinkPerformance ###");
		ConnectorUtils.showPerformanceResults(testResults1);
		System.out.println("\n### Results - testSourceAndSinkPerformance2 ###");
		ConnectorUtils.showPerformanceResults(testResults2);
		System.out.println("\n### Results - testSourceAndSinkPerformance3 ###");
		ConnectorUtils.showPerformanceResults(testResults3);
	}
}
