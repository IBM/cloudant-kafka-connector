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
        
        //####TASKS####
        //LOG.info("Thread[" + Thread.currentThread().getId() + "].sinkRecords = " + sinkRecords.size());
		System.out.println("Thread[" + Thread.currentThread().getId() + "].sinkRecords = " + sinkRecords.size());
		
		//TODO:create Threads for all topics with task.max
		for (SinkRecord record : sinkRecords) {
		
			JSONObject jsonRecord;
		
			JSONTokener tokener = new JSONTokener(record.value().toString());		
			jsonRecord = new JSONObject(tokener);
											
			if (jsonRecord.has(CloudantConst.CLOUDANT_REV)) {
				jsonRecord.remove(CloudantConst.CLOUDANT_REV);
			}
			
			if(jsonRecord.has(CloudantConst.CLOUDANT_DOC_ID)){			
				//Create guid_schema depending from property guid.schema
				if(CloudantSinkTask.guid_schema.equalsIgnoreCase(InterfaceConst.GUID_SETTING.KAFKA.name())) {				
					jsonRecord.put(CloudantConst.CLOUDANT_DOC_ID, 
							record.topic() + "_" + 
							record.kafkaPartition().toString() + "_" + 
							Long.toString(record.kafkaOffset()) + "_" + 
							jsonRecord.get(CloudantConst.CLOUDANT_DOC_ID));	
				}
				else if (CloudantSinkTask.guid_schema.equalsIgnoreCase(InterfaceConst.GUID_SETTING.CLOUDANT.name())) {
					// Do Nothing => Mirror from Cloundant Obj
				}
				else {
					//LOG.info(MessageKey.GUID_SCHEMA + ": " + guid_schema);
					//LOG.warn(CloudantConst.CLOUDANT_DOC_ID + "from source database will removed");
					
					//remove Cloudant _id
					jsonRecord.remove(CloudantConst.CLOUDANT_DOC_ID);
				}
			}					
			CloudantSinkTask.jsonArray.put(jsonRecord);
			
			if ((CloudantSinkTask.jsonArray != null) && (CloudantSinkTask.jsonArray.length() >= CloudantSinkTask.batch_size)) {
	
				//flush(null);
	
			} 
		} 
        
        
	}
}