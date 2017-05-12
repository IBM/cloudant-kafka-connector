package com.ibm.cloudant.kafka.connect;

import java.util.Collection;

import org.apache.kafka.connect.sink.SinkRecord;
import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONTokener;

import com.ibm.cloudant.kafka.common.CloudantConst;
import com.ibm.cloudant.kafka.common.InterfaceConst;
import com.ibm.cloudant.kafka.common.MessageKey;

public class CloudantSinkRecordReader implements Runnable{
	public static volatile int counter = 0;
	private Collection<SinkRecord> sinkRecords;	
	
	public CloudantSinkRecordReader(Collection<SinkRecord> sinkRecords) {
		this.sinkRecords = sinkRecords;
	}
	
	public void run() {
		counter++;
        System.out.println(Thread.currentThread().getName() + " => Counter: " + counter); 
        System.out.println("Size SinkRecords: " + this.sinkRecords.size());        
	}
}