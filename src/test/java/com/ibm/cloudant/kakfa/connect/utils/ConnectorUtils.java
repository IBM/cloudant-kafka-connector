package com.ibm.cloudant.kakfa.connect.utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.connect.connector.ConnectorContext;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.easymock.EasyMock;

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
	
	public static void showPerformanceResults(long documents, long diskSize, ArrayList<Long> timeDiff) {		
		float diskSizeInMB = diskSize/(1024*1024);
				
		System.out.println("Documents: " + documents);
		System.out.println("DB Size: " + diskSizeInMB + " MB");
		System.out.println("Time: " + average(timeDiff)/1000f + " Seconds");
		System.out.println("Documents per second: " + documents/(average(timeDiff)/1000f));
		System.out.println("Data per second: " + diskSizeInMB/(average(timeDiff)/1000f) + " MB/s");		
	}
	
	public static double average(ArrayList<Long> list) {
	    // 'average' is undefined if there are no elements in the list.
	    if (list == null || list.isEmpty())
	        return 0.0;
	    // Calculate the summation of the elements in the list
	    long sum = 0;
	    int n = list.size();
	    // Iterating manually is faster than using an enhanced for loop.
	    for (int i = 0; i < n; i++)
	        sum += list.get(i);
	    // We don't want to perform an integer division, so the cast is mandatory.
	    return ((double) sum) / n;
	}
}
