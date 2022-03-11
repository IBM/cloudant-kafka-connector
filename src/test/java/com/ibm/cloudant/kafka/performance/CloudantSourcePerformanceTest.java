/*
 * Copyright © 2017, 2022 IBM Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */
package com.ibm.cloudant.kafka.performance;

import com.carrotsearch.junitbenchmarks.AbstractBenchmark;
import com.carrotsearch.junitbenchmarks.BenchmarkOptions;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.ibm.cloud.cloudant.v1.Cloudant;
import com.ibm.cloud.cloudant.v1.model.DatabaseInformation;
import com.ibm.cloudant.kafka.common.InterfaceConst;
import com.ibm.cloudant.kafka.common.utils.JavaCloudantUtil;
import com.ibm.cloudant.kafka.connect.CloudantSourceConnector;
import com.ibm.cloudant.kafka.connect.CloudantSourceTask;
import com.ibm.cloudant.kafka.connect.utils.CloudantDbUtils;
import com.ibm.cloudant.kafka.connect.utils.ConnectorUtils;

import org.apache.kafka.connect.connector.ConnectorContext;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.powermock.api.easymock.PowerMock;

import java.util.List;
import java.util.Map;

public class CloudantSourcePerformanceTest extends AbstractBenchmark {	
	private static Cloudant sourceService;
	private static JsonObject testResults1 = new JsonObject();
	private static JsonObject testResults2 = new JsonObject();
	private static JsonObject testResults3 = new JsonObject();

	private Map<String, String> sourceProperties;

    private CloudantSourceConnector sourceConnector;

    @Before
	public void setUp() throws Exception {	
		//Set properties
		
		sourceProperties = ConnectorUtils.getSourceProperties();
		// Update to use the performance URL
		sourceProperties.put(InterfaceConst.URL, sourceProperties.get(ConnectorUtils.PERFORMANCE_URL));
		
		// Get the source database handle
		sourceService = JavaCloudantUtil.getClientInstance(
			sourceProperties.get(InterfaceConst.URL),
			sourceProperties.get(InterfaceConst.USER_NAME),
			sourceProperties.get(InterfaceConst.PASSWORD));
			
		sourceConnector = new CloudantSourceConnector();
        ConnectorContext context = PowerMock.createMock(ConnectorContext.class);
        sourceConnector.initialize(context);
	}
					
	@BenchmarkOptions(benchmarkRounds = 3, warmupRounds = 0)
	@Test
	public void testSourcePerformance() throws Exception {
		//set parameter => init(topic, batch.size, tasks.max)
		init("t1", 10000, 1);
		long testTime = runTest();
		testResults1 = addResults(testResults1, testTime);
	}
	
	@BenchmarkOptions(benchmarkRounds = 3, warmupRounds = 0)
	@Test
	public void testSourcePerformance2() throws Exception {
		//set parameter => init(topic, batch.size, tasks.max)
		init("t2", 10000, 1);
		long testTime = runTest();
		testResults2 = addResults(testResults2, testTime);
	}
	
	@BenchmarkOptions(benchmarkRounds = 3, warmupRounds = 0)
	@Test
	public void testSourcePerformance3() throws Exception {
		//set parameter => init(topic, batch.size, tasks.max)
		init("t3", 10000, 1);
		long testTime = runTest();
		testResults3 = addResults(testResults3, testTime);
	}
	
	private void init(String topics, int batch_size, int tasks_max) {
		sourceProperties.put(InterfaceConst.TOPIC, topics);
		sourceProperties.put(InterfaceConst.BATCH_SIZE, Integer.toString(batch_size));
		sourceProperties.put(InterfaceConst.TASKS_MAX, Integer.toString(tasks_max));
	}
	
	private JsonObject addResults(JsonObject results, long testTime) {
		if(results.size() == 0){			
			JsonArray testTimes = new JsonArray();
			testTimes.add(testTime);								
			results.addProperty("testRounds", 1);
			DatabaseInformation dbInfo = CloudantDbUtils.getDbInfo(sourceProperties.get(InterfaceConst.DB), sourceService);
			results.addProperty("diskSize", dbInfo.getSizes().getFile());
			results.addProperty("documents",dbInfo.getDocCount());
			
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
	
	private long runTest() throws Exception {
		sourceConnector.start(sourceProperties);       
                      
		// 1. Create Connector and Trigger sourceTask to get a batch of records		
        CloudantSourceTask sourceTask = ConnectorUtils.createCloudantSourceConnector(sourceProperties);
		sourceTask.start(sourceProperties);		
		List<SourceRecord> records;
		
		// 2. Measure SourceRecords
		long startTime = System.currentTimeMillis();		
		       
		do {			
			// 2a. Get a batch of source records
			records = sourceTask.poll();
		} while (records.size() > 0);
				
		//3. Stop sourceTask		
		sourceTask.stop();
		
		long endTime = System.currentTimeMillis();
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
