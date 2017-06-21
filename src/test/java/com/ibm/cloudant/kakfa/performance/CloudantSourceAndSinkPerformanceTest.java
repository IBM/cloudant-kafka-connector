package com.ibm.cloudant.kakfa.performance;

import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.connect.connector.ConnectorContext;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.powermock.api.easymock.PowerMock;

import com.carrotsearch.junitbenchmarks.AbstractBenchmark;
import com.carrotsearch.junitbenchmarks.BenchmarkOptions;
import com.cloudant.client.api.Database;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.ibm.cloudant.kafka.common.InterfaceConst;
import com.ibm.cloudant.kafka.common.utils.JavaCloudantUtil;
import com.ibm.cloudant.kafka.connect.CloudantSinkConnector;
import com.ibm.cloudant.kafka.connect.CloudantSinkTask;
import com.ibm.cloudant.kafka.connect.CloudantSourceConnector;
import com.ibm.cloudant.kafka.connect.CloudantSourceTask;
import com.ibm.cloudant.kakfa.connect.utils.CloudantDbUtils;
import com.ibm.cloudant.kakfa.connect.utils.ConnectorUtils;

public class CloudantSourceAndSinkPerformanceTest extends AbstractBenchmark {	
	private static Database targetDb;	
	private static JsonObject testResults1 = new JsonObject();
	private static JsonObject testResults2 = new JsonObject();
	private static JsonObject testResults3 = new JsonObject();
		
	private Properties defaultProperties;
	private Map<String, String> sourceProperties;  	
	private Map<String, String> targetProperties; 
	
	private CloudantSinkConnector sinkConnector;
	private CloudantSourceConnector sourceConnector;
	private ConnectorContext context;
	
	private CloudantSinkTask sinkTask;
	private CloudantSourceTask sourceTask;
	
	private List<SourceRecord> sourceRecords;
	private List<SinkRecord> sinkRecords;
	private List<SinkRecord> tempRecords;
	
	private AtomicBoolean _runningSourceThread;
	private AtomicBoolean _runningSinkThread;
	
	@Before
	public void setUp() throws Exception {	
		//Set properties
		defaultProperties = new Properties();
		defaultProperties.load(new FileReader(new File("src/test/resources/test.properties")));
		
		sourceProperties = ConnectorUtils.getSourceProperties(defaultProperties);
		sourceProperties.put(InterfaceConst.URL, defaultProperties.getProperty("performance.url"));
		targetProperties = ConnectorUtils.getTargetProperties(defaultProperties);
		
		//Create SinkConnector
		sinkConnector = new CloudantSinkConnector();
        context = PowerMock.createMock(ConnectorContext.class);
        sinkConnector.initialize(context);
		
		//Create SourceConnector
        sourceConnector = new CloudantSourceConnector();
        context = PowerMock.createMock(ConnectorContext.class);
        sourceConnector.initialize(context);
			
		// Create a _target database to replicate data into	
		targetDb = JavaCloudantUtil.getDBInst(defaultProperties.getProperty("performance.url") + "_target", 
				defaultProperties.get(InterfaceConst.USER_NAME).toString(),
				defaultProperties.get(InterfaceConst.PASSWORD).toString());
		
		//Create Sink Connector	
		sourceTask = new CloudantSourceTask();		
		sinkTask = new CloudantSinkTask();
	}
	
	@BenchmarkOptions(benchmarkRounds = 3, warmupRounds = 0)
	@Test
	public void testSourceAndSinkPerformance() throws Exception {
		//set parameter => init(topic, batch.size, tasks.max, guid.schema)
		init("t01", 10000, 1, false);
		long testTime = runTest();			
		testResults1 = addResults(testResults1, testTime);
	}
	
	@BenchmarkOptions(benchmarkRounds = 3, warmupRounds = 0)
	@Test
	public void testSourceAndSinkPerformance2() throws Exception {
		//set parameter => init(topic, batch.size, tasks.max, guid.schema)
		init("t02", 10000, 1, false);
		long testTime = runTest();
		testResults2 = addResults(testResults2, testTime);
	}
	
	@BenchmarkOptions(benchmarkRounds = 3, warmupRounds = 0)
	@Test
	public void testSourceAndSinkPerformance3() throws Exception {
		//set parameter => init(topic, batch.size, tasks.max, guid.schema)
		init("t03", 10000, 1, false);
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
		// 1. Set variables								
		sinkRecords = new ArrayList<SinkRecord>();
		sourceRecords = new ArrayList<SourceRecord>();	
		tempRecords = Collections.synchronizedList(new ArrayList<SinkRecord>());
		
		_runningSourceThread = new AtomicBoolean(true);
		_runningSinkThread = new AtomicBoolean(true);	
		
		// 2. Start source and sink task
		sourceTask.start(sourceProperties);
		sinkTask.start(targetProperties);	
									
		// 3. Thread for source tasks
		Thread threadSourceTask = new Thread() {			  			
			public void run() {		
				  try {	  
					  do {				  								
						  sourceRecords = sourceTask.poll();	
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
						  	
						  	synchronized(tempRecords){
							  tempRecords.addAll(sinkRecords);
						  	}				  					  
						  	sinkRecords.clear();
					  
					  } while (sourceRecords.size() > 0);
					  
					  _runningSourceThread.set(false);				  
				  } catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				  }					  
			  	}
		};
				
		// 4. Thread for sink tasks
		Thread threadSinkTask = new Thread() {
		  public void run() {		  					    		    		  
			  try {				  	
				  do {	  					  			  
					  if((tempRecords.size() == 0)) {
						  Thread.sleep(100);
					  }
					  else {		 					  						  
						  synchronized(tempRecords){
							  sinkTask.put(tempRecords);
							  tempRecords.clear();
						  }							  
					  }
				  } while (sourceRecords.size() > 0 || tempRecords.size() > 0 || targetDb.info().getDocCount() == 0);
				  
				  _runningSinkThread.set(false);			  
			  } catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
			  }		  
		  	}		  			
		 };
								
		// 5. Start Threads and measure time
		long startTime = System.currentTimeMillis();	
		
		threadSourceTask.start(); 
		threadSinkTask.start();
		 
		// 5. Test Thread are running	
		while (_runningSourceThread.get() || _runningSinkThread.get()) {		
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				//LOG.error(e);
			}
		}	
		
		long endTime = System.currentTimeMillis();
			
		// 6. Stop Threads
		threadSourceTask.interrupt();
		threadSinkTask.interrupt();
		
		// 7. Flush the latest set of records in case the batch has not been committed
		sinkTask.flush(null);
		sinkRecords.clear();
		
		// 8. Stop source and target task				
		sourceTask.stop();
		sinkTask.stop();
		return endTime - startTime;							
	}
	
	public JsonObject addResults(JsonObject results, long testTime) {
		if(results.size() == 0){			
			JsonArray testTimes = new JsonArray();
			testTimes.add(testTime);								
			results.addProperty("testRounds", 1);
			results.addProperty("diskSize", targetDb.info().getDiskSize());
			results.addProperty("documents", targetDb.info().getDocCount());
			
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