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

import java.io.IOException;
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
	
	private JSONArray jsonArray = new JSONArray();
	private static int PAGE_SIZE = 1;
	
	public String version() {
		 return new CloudantSinkConnector().version();
	}

	@Override
	public void put(Collection<SinkRecord> sinkRecords) {
	
		JSONArray jsonArray = new JSONArray();

		for (SinkRecord record : sinkRecords) {
			LOG.debug("Add document to: " +  url + " <- " + (String) record.value().toString());
			JSONObject jsonRecord;
		
			JSONTokener tokener = new JSONTokener(record.value().toString());		
			jsonRecord = new JSONObject(tokener);
			
			String _id = JsonUtil.getStringValue(jsonRecord, CloudantConst.CLOUDANT_DOC_ID);
			
			if (jsonRecord.has(CloudantConst.CLOUDANT_REV)) {
				jsonRecord.remove(CloudantConst.CLOUDANT_REV);
			}
			
			jsonArray.put(jsonRecord);
			
			LOG.info("DOCUMENT: " + _id);
			
			if (jsonArray.length() == PAGE_SIZE) {
				try {
					if (jsonArray.length() > 0) {
						LOG.info("Commit " + jsonArray.length() + " documents to -> " + url);
					}
				
					JSONArray results = JavaCloudantUtil.batchWrite(url, userName, password, jsonArray);
					
					if (results != null) {
						for (int i = 0; i < results.length(); i++) {
							JSONObject result = (JSONObject) results.get(i);
							LOG.info(result.toString());
						}
					}
				} catch (JSONException e) {
					LOG.error(e.getMessage(), e);
				}
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
		
		} catch (ConfigException e) {
			throw new ConnectException(ResourceBundleUtil.get(MessageKey.CONFIGURATION_EXCEPTION), e);
		}

	}

	@Override
	public void flush(Map<TopicPartition, org.apache.kafka.clients.consumer.OffsetAndMetadata> offsets) {
		LOG.debug("Flushing output stream for {" + url + "}");
		
		try {
			JavaCloudantUtil.batchWrite(url, userName, password, jsonArray);
			if (jsonArray.length() > 0) {
				LOG.info("Committed " + jsonArray.length() + " documents to -> " + url);
			}
		} catch (JSONException e) {
			LOG.error(e.getMessage(), e);
		}	
	}
	
}
