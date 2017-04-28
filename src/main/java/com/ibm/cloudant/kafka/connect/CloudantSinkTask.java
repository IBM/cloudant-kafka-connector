/*******************************************************************************
* Copyright (c) 2016 IBM Corp.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*******************************************************************************/
package com.ibm.cloudant.kafka.connect;

import java.util.Collection;
import java.util.Map;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONTokener;

import com.ibm.cloudant.kafka.common.CloudantConst;
import com.ibm.cloudant.kafka.common.InterfaceConst;
import com.ibm.cloudant.kafka.common.MessageKey;
import com.ibm.cloudant.kafka.common.utils.JavaCloudantUtil;
import com.ibm.cloudant.kafka.common.utils.JsonUtil;
import com.ibm.cloudant.kafka.common.utils.ResourceBundleUtil;


public class CloudantSinkTask extends SinkTask {
	
	private static Logger LOG = Logger.getLogger(CloudantSinkTask.class);
	
	private CloudantSinkTaskConfig config;
	
	private String url = null;
	private String userName = null;
	private String password = null;
	
	private static int batch_size = 0;
	private String guid_schema = null;
	private JSONArray jsonArray = new JSONArray();
		
	public String version() {
		 return new CloudantSinkConnector().version();
	}

	@Override
	public void put(Collection<SinkRecord> sinkRecords) {
	
		LOG.info("Thread[" + Thread.currentThread().getId() + "].sinkRecords = " + sinkRecords.size());
		
		for (SinkRecord record : sinkRecords) {
		
			JSONObject jsonRecord;
		
			JSONTokener tokener = new JSONTokener(record.value().toString());		
			jsonRecord = new JSONObject(tokener);
											
			if (jsonRecord.has(CloudantConst.CLOUDANT_REV)) {
				jsonRecord.remove(CloudantConst.CLOUDANT_REV);
			}
			
			if(jsonRecord.has(CloudantConst.CLOUDANT_DOC_ID)){
				if(guid_schema.equalsIgnoreCase(InterfaceConst.GUID_SETTING.KAFKA.name())) {				
					jsonRecord.put(CloudantConst.CLOUDANT_DOC_ID, 
							record.topic() + "_" + 
							record.kafkaPartition().toString() + "_" + 
							Long.toString(record.kafkaOffset()) + "_" + 
							jsonRecord.get(CloudantConst.CLOUDANT_DOC_ID));	
				}
				else if (guid_schema.equalsIgnoreCase(InterfaceConst.GUID_SETTING.CLOUDANT.name())) {
					// Do Nothing => Mirror from Cloundant Obj
				}
				else {
					LOG.info(MessageKey.GUID_SCHEMA + ": " + guid_schema);
					LOG.warn(CloudantConst.CLOUDANT_DOC_ID + "from source database will removed");
					
					//remove Cloudant _id
					jsonRecord.remove(CloudantConst.CLOUDANT_DOC_ID);
				}
			}					
			jsonArray.put(jsonRecord);
			
			if ((jsonArray != null) && (jsonArray.length() >= batch_size)) {
	
				flush(null);
	
			} 
		} 
	}


	@Override
	public void stop() {
		// reader.finish();
	}

	
 	@Override
	public void start(Map<String, String> props) {
 		
 		try {
			config = new CloudantSinkTaskConfig(props);
			
			url = config.getString(InterfaceConst.URL);
			userName = config.getString(InterfaceConst.USER_NAME);
			password = config.getString(InterfaceConst.PASSWORD);
			
			batch_size = config.getInt(InterfaceConst.BATCH_SIZE)==null ? CloudantConst.DEFAULT_BATCH_SIZE : config.getInt(InterfaceConst.BATCH_SIZE);
			guid_schema = config.getString(InterfaceConst.GUID_SCHEMA) == null ? InterfaceConst.DEFAULT_GUID_SETTING : config.getString(InterfaceConst.GUID_SCHEMA); 

		} catch (ConfigException e) {
			throw new ConnectException(ResourceBundleUtil.get(MessageKey.CONFIGURATION_EXCEPTION), e);
		}

	}

	@Override
	public void flush(Map<TopicPartition, org.apache.kafka.clients.consumer.OffsetAndMetadata> offsets) {
		LOG.debug("Flushing output stream for {" + url + "}");

		try {

			if ((jsonArray != null) && (jsonArray.length() > 0)) {

				JSONArray results = JavaCloudantUtil.batchWrite(url, userName, password, jsonArray);
				LOG.info("Committed " + jsonArray.length() + " documents to -> " + url);

				// The results array has a record for every single document commit
				// Processing this is expensive!
				if (results != null) {
					/* 
					for (int i = 0; i < results.length(); i++) {
						JSONObject result = (JSONObject) results.get(i);
						LOG.debug(result.toString());
					}
					*/
				}
			}

		} catch (JSONException e) {
			LOG.error(e.getMessage(), e);
		} finally {
			// Release memory (regardless if documents got committed or not)
			jsonArray = new JSONArray(); ;
		}
	}

}
