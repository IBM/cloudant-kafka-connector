package com.ibm.cloudant.kakfa.connect.utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.connect.connector.ConnectorContext;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.easymock.EasyMock;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.ibm.cloudant.kafka.common.InterfaceConst;
import com.ibm.cloudant.kafka.connect.CloudantSourceConnector;
import com.ibm.cloudant.kafka.connect.CloudantSourceTask;

public class ConnectorUtils {			
	public static CloudantSourceTask createCloudantSourceConnector(Map<String, String> properties) {	
		CloudantSourceConnector connector = new CloudantSourceConnector();		
		CloudantSourceTask task = new CloudantSourceTask(); 
		
		ConnectorContext context = EasyMock.mock(ConnectorContext.class);
		SourceTaskContext taskContext = EasyMock.mock(SourceTaskContext.class);	
		
		connector.initialize(context);
		connector.start(properties);

		task.initialize(taskContext);
		return task;
	}
	
	public static Map<String, String> getSourceProperties(Properties testProperties) {		
		Map<String, String> sourceProperties = new HashMap<String, String>();
		sourceProperties.put(InterfaceConst.URL,testProperties.getProperty(InterfaceConst.URL));
		sourceProperties.put(InterfaceConst.USER_NAME, testProperties.getProperty(InterfaceConst.USER_NAME));
		sourceProperties.put(InterfaceConst.PASSWORD, testProperties.getProperty(InterfaceConst.PASSWORD));

		sourceProperties.put(InterfaceConst.TOPIC, testProperties.getProperty(InterfaceConst.TOPIC));
		sourceProperties.put(InterfaceConst.TASKS_MAX, testProperties.getProperty(InterfaceConst.TASKS_MAX));
		sourceProperties.put(InterfaceConst.BATCH_SIZE, testProperties.getProperty(InterfaceConst.BATCH_SIZE)==null?"500":testProperties.getProperty(InterfaceConst.BATCH_SIZE));

		sourceProperties.put(InterfaceConst.LAST_CHANGE_SEQ, testProperties.getProperty(InterfaceConst.LAST_CHANGE_SEQ));		
		
		return sourceProperties;
	}
	
	public static Map<String, String> getTargetProperties(Properties testProperties) {		
		Map<String, String> targetProperties = new HashMap<String, String>();
		targetProperties.put(InterfaceConst.URL, testProperties.getProperty(InterfaceConst.URL));
		targetProperties.put(InterfaceConst.USER_NAME, testProperties.getProperty(InterfaceConst.USER_NAME));
		targetProperties.put(InterfaceConst.PASSWORD, testProperties.getProperty(InterfaceConst.PASSWORD));
	
		targetProperties.put(InterfaceConst.TASKS_MAX, testProperties.getProperty(InterfaceConst.TASKS_MAX));
		targetProperties.put(InterfaceConst.BATCH_SIZE, testProperties.getProperty(InterfaceConst.BATCH_SIZE));
		  
		targetProperties.put(InterfaceConst.TOPIC, testProperties.getProperty(InterfaceConst.TOPIC));
		
		targetProperties.put(InterfaceConst.GUID_SCHEMA, testProperties.getProperty(InterfaceConst.GUID_SCHEMA));
		
		return targetProperties;
	}
	
	public static void showPerformanceResults(JsonObject testResults) {		
		float diskSizeInMB = testResults.get("diskSize").getAsLong()/(1024*1024);
				
		//Show TestProperties			
		System.out.println("Rounds: " + testResults.get("testRounds") + "; Documents: " + testResults.get("documents") + "; DB Size: " + diskSizeInMB + " MB");						
		System.out.print(InterfaceConst.TOPIC + ": " + testResults.get(InterfaceConst.TOPIC));
		System.out.print("; " + InterfaceConst.BATCH_SIZE + ": " + testResults.get(InterfaceConst.BATCH_SIZE));
		System.out.print("; " + InterfaceConst.TASKS_MAX + ": " + testResults.get(InterfaceConst.TASKS_MAX));
		if(testResults.has(InterfaceConst.GUID_SCHEMA)) System.out.print("; " + InterfaceConst.GUID_SCHEMA + ": " + testResults.get(InterfaceConst.GUID_SCHEMA));
		
		//Show TestResults
		System.out.print("\nTime: " + average(getTestTimes(testResults))/1000f + " Seconds");
		System.out.println("[+- " + stdDeviation(getTestTimes(testResults))/1000f + "]");
		System.out.println("Documents per second: " + testResults.get("documents").getAsLong()/(average(getTestTimes(testResults))/1000f));
		System.out.println("Data per second: " + diskSizeInMB/(average(getTestTimes(testResults))/1000f) + " MB/s");		
	}
	
	public static ArrayList<Long> getTestTimes(JsonObject testResults) {
		ArrayList<Long> list = new ArrayList<Long>(); 
		JsonArray testTimes = testResults.get("testTimes").getAsJsonArray();
		if (testTimes != null) { 
		   for (int i=0;i<testTimes.size();i++){ 
			   list.add(testTimes.get(i).getAsLong());
		   } 
		}
		return list;
	}
	
	public static double average(ArrayList<Long> list) {			
	    if (list == null || list.isEmpty()) return 0.0;
	    
	    long sum = 0;
	    for (int i = 0; i < list.size(); i++) {
	    	sum += list.get(i);
	    }
	        
	    return ((double) sum) / list.size();
	}
	
	public static double stdDeviation(ArrayList<Long> list) {		
	   double sumDiffsSquared = 0.0;
	   double avg = average(list);
	   for (long value : list) {
	       double diff = value - avg;
	       diff *= diff;
	       sumDiffsSquared += diff;
	   }
	   return Math.sqrt(sumDiffsSquared  / (list.size()-1));
	}
}