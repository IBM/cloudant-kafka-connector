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
import com.carrotsearch.junitbenchmarks.annotation.AxisRange;
import com.carrotsearch.junitbenchmarks.annotation.BenchmarkMethodChart;
import com.cloudant.client.api.Database;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.ibm.cloudant.kafka.common.InterfaceConst;
import com.ibm.cloudant.kafka.common.utils.JavaCloudantUtil;
import com.ibm.cloudant.kafka.connect.CloudantSinkTask;
import com.ibm.cloudant.kakfa.connect.utils.CloudantDbUtils;
import com.ibm.cloudant.kakfa.connect.utils.ConnectorUtils;

@AxisRange(min = 0, max = 5)
@BenchmarkMethodChart(filePrefix = "benchmark-lists")
public class CloudantSinkPerformanceTest extends AbstractBenchmark {	
	private static Database targetDb;	
	private static ArrayList<Long> timeTest1 = new ArrayList<Long>();	
	private static ArrayList<Long> timeTest2 = new ArrayList<Long>();
	private static ArrayList<Long> timeTest3 = new ArrayList<Long>();
	private static long diskSize, documents;
		
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
		
		//Create Connectors	
		sinkTask = new CloudantSinkTask();		
		
		//Create Kafka SinkRecord
		sinkRecords = new ArrayList<SinkRecord>();
	}
	
	@BenchmarkOptions(benchmarkRounds = 3, warmupRounds = 0)
	@Test
	public void testSinkPerformance() throws Exception {
		init(100000, "perfomanceTest", 5000, 2, "kafka");
		timeTest1.add(runTest());
	}
	
	@BenchmarkOptions(benchmarkRounds = 3, warmupRounds = 0)
	@Test
	public void testSinkPerformance2() throws Exception {
		init(100000, "perfomanceTest", 5000, 2, "kafka");
		timeTest2.add(runTest());
	}
	
	@BenchmarkOptions(benchmarkRounds = 3, warmupRounds = 0)
	@Test
	public void testSinkPerformance3() throws Exception {
		init(100000, "perfomanceTest", 5000, 2, "kafka");
		timeTest3.add(runTest());
	}
	
	public void init(int numberDocs, String topics, int batch_size, int tasks_max, String guid_schema) {
		JsonParser parser = new JsonParser();
			
		for(int i=0; i<numberDocs; i++) {
			JsonObject doc = parser.parse("{\"_id\":\"doc" + i + "\","
					+ "\"key\":\"value"+ i +"\"}").getAsJsonObject();
					
			sinkRecords.add(new SinkRecord(defaultProperties.getProperty(InterfaceConst.TOPIC), 0, 
							null, null, Schema.STRING_SCHEMA, doc, i));	
		}		
		
		targetProperties.put(InterfaceConst.URL, defaultProperties.getProperty("performance.url") + "_target");
		targetProperties.put(InterfaceConst.TOPIC, topics); //ToDO: mehrere Topics
		targetProperties.put(InterfaceConst.BATCH_SIZE, Integer.toString(batch_size));
		targetProperties.put(InterfaceConst.TASK_NUMBER, Integer.toString(tasks_max));
		targetProperties.put(InterfaceConst.TASKS_MAX, Integer.toString(tasks_max));
		targetProperties.put(InterfaceConst.GUID_SCHEMA, guid_schema);
	}
	
	public long runTest() throws Exception {									
		// KAFKA
		HashMap<TopicPartition, OffsetAndMetadata> offsets = new HashMap<TopicPartition, OffsetAndMetadata>();		
		
		// 1. Create Connector and Trigger sourceTask to get a batch of records		
		sinkTask.start(targetProperties);			
			
		// 2. Measure SourceRecords
		long startTime = System.currentTimeMillis();				
		
		// Do Something
		sinkTask.put(sinkRecords);
		sinkTask.flush(offsets);
		
		long endTime = System.currentTimeMillis();
		
		//stop sourceTask		
		sinkTask.stop();
		
		return endTime - startTime;
	}
				
	@After
	public void tearDown() throws Exception { 
		diskSize = targetDb.info().getDiskSize();
		documents = targetDb.info().getDocCount();
		
		CloudantDbUtils.dropDatabase(
				defaultProperties.getProperty("performance.url") + "_target", 
				defaultProperties.get(InterfaceConst.USER_NAME).toString(), 
				defaultProperties.get(InterfaceConst.PASSWORD).toString());
	}
	
	@AfterClass
	public static void Results() {
		//Show Properties and BenchmarkRounds
		System.out.println("\n### Results - PerformanceSinkTest1 ###");
		ConnectorUtils.showPerformanceResults(documents, diskSize, timeTest1);
		System.out.println("\n### Results - PerformanceSinkTest2 ###");
		ConnectorUtils.showPerformanceResults(documents, diskSize, timeTest2);
		System.out.println("\n### Results - PerformanceSinkTest3 ###");
		ConnectorUtils.showPerformanceResults(documents, diskSize, timeTest3);
	}
}
