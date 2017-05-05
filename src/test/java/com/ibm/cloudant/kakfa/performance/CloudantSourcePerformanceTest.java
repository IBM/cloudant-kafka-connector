package com.ibm.cloudant.kakfa.performance;

import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.connect.source.SourceRecord;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

import com.carrotsearch.junitbenchmarks.AbstractBenchmark;
import com.carrotsearch.junitbenchmarks.BenchmarkOptions;
import com.carrotsearch.junitbenchmarks.annotation.AxisRange;
import com.carrotsearch.junitbenchmarks.annotation.BenchmarkMethodChart;
import com.cloudant.client.api.Database;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.ibm.cloudant.kafka.common.InterfaceConst;
import com.ibm.cloudant.kafka.common.utils.JavaCloudantUtil;
import com.ibm.cloudant.kafka.connect.CloudantSourceTask;
import com.ibm.cloudant.kakfa.connect.utils.ConnectorUtils;

@AxisRange(min = 0, max = 5)
@BenchmarkMethodChart(filePrefix = "benchmark-lists")
public class CloudantSourcePerformanceTest extends AbstractBenchmark {	
	private static Database sourceDb;
	private static JsonObject testResults1 = new JsonObject();
	private static JsonObject testResults2 = new JsonObject();
	private static JsonObject testResults3 = new JsonObject();
	
	private Properties defaultProperties;
	private Map<String, String> sourceProperties;  	
	private CloudantSourceTask sourceTask;	
				
	@Before
	public void setUp() throws Exception {	
		//Set properties
		defaultProperties = new Properties();
		defaultProperties.load(new FileReader(new File("src/test/resources/test.properties")));
		
		sourceProperties = ConnectorUtils.getSourceProperties(defaultProperties);
		sourceProperties.put(InterfaceConst.URL, defaultProperties.getProperty("performance.url"));
		
		// Get the source database handle
		sourceDb = JavaCloudantUtil.getDBInst(
				defaultProperties.getProperty("performance.url"), 
				defaultProperties.get(InterfaceConst.USER_NAME).toString(),
				defaultProperties.get(InterfaceConst.PASSWORD).toString());	
	}
					
	@BenchmarkOptions(benchmarkRounds = 3, warmupRounds = 0)
	@Test
	public void testSourcePerformance() throws Exception {
		init("perfomanceTest", 1000, 1);
		long testTime = runTest();
		testResults1 = addResults(testResults1, testTime);
	}
	
	@BenchmarkOptions(benchmarkRounds = 3, warmupRounds = 0)
	@Test
	public void testSourcePerformance2() throws Exception {
		init("perfomanceTest", 2000, 1);
		long testTime = runTest();
		testResults2 = addResults(testResults2, testTime);
	}
	
	@BenchmarkOptions(benchmarkRounds = 3, warmupRounds = 0)
	@Test
	public void testSourcePerformance3() throws Exception {
		init("perfomanceTest", 10000, 1);
		long testTime = runTest();
		testResults3 = addResults(testResults3, testTime);
	}
	
	public void init(String topics, int batch_size, int tasks_max) {
		sourceProperties.put(InterfaceConst.URL, defaultProperties.getProperty("performance.url"));
		sourceProperties.put(InterfaceConst.TOPIC, topics); //ToDO: mehrere Topics
		sourceProperties.put(InterfaceConst.BATCH_SIZE, Integer.toString(batch_size));
		sourceProperties.put(InterfaceConst.TASKS_MAX, Integer.toString(tasks_max));
	}
	
	public JsonObject addResults(JsonObject results, long testTime) {
		if(results.size() == 0){			
			JsonArray testTimes = new JsonArray();
			testTimes.add(testTime);								
			results.addProperty("testRounds", 1);
			results.addProperty("diskSize", sourceDb.info().getDiskSize());
			results.addProperty("documents", sourceDb.info().getDocCount());
			
			results.addProperty(InterfaceConst.TOPIC, sourceProperties.get(InterfaceConst.TOPIC));
			results.addProperty(InterfaceConst.BATCH_SIZE, sourceProperties.get(InterfaceConst.BATCH_SIZE));
			results.addProperty(InterfaceConst.TASKS_MAX, sourceProperties.get(InterfaceConst.TASKS_MAX));
			results.add("testTimes", testTimes);	
		}
		else {
			results.addProperty("testRounds", results.get("testRounds").getAsInt() + 1);
			results.get("testTimes").getAsJsonArray().add(testTime);
		}
		return results;
	}
	
	public long runTest() throws Exception {									
		// 1. Create Connector and Trigger sourceTask to get a batch of records		
		sourceTask = ConnectorUtils.createCloudantSourceConnector(sourceProperties);
		sourceTask.start(sourceProperties);		
		List<SourceRecord> records = new ArrayList<SourceRecord>();
		
		// 2. Measure SourceRecords
		long startTime = System.currentTimeMillis();		
		do {			
			// 2a. Get a batch of source records
			records = sourceTask.poll();
		} while (records.size() > 0);			
		long endTime = System.currentTimeMillis();
		
		//stop sourceTask		
		sourceTask.stop();
		
		return endTime - startTime;
	}
	
	@AfterClass
	public static void Results() {
		System.out.println("\n### Results - testSourcePerformance ###");
		ConnectorUtils.showPerformanceResults(testResults1);
		System.out.println("\n### Results - testSourcePerformance2 ###");
		ConnectorUtils.showPerformanceResults(testResults2);
		System.out.println("\n### Results - testSourcePerformance3 ###");
		ConnectorUtils.showPerformanceResults(testResults3);
	}
}
