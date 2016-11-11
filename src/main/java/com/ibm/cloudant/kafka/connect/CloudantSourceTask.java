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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.apache.log4j.Logger;
import org.json.JSONObject;

import com.ibm.cloudant.kafka.common.CloudantConst;
import com.ibm.cloudant.kafka.common.InterfaceConst;
import com.ibm.cloudant.kafka.common.MessageKey;
import com.ibm.cloudant.kafka.common.utils.JavaCloudantOneTimeFeed;
import com.ibm.cloudant.kafka.common.utils.JsonUtil;
import com.ibm.cloudant.kafka.common.utils.ResourceBundleUtil;
import com.ibm.cloudant.kafka.common.utils.UnitOfWorkManager;

import org.apache.kafka.common.config.ConfigException;

public class CloudantSourceTask extends SourceTask {
	
	private static Logger LOG = Logger.getLogger(CloudantSourceTask.class);
	
	private UnitOfWorkManager uowManager;


	private static int CLOUDANT_BATCH_SIZE = 10;
	
	int docCounter = 0;
	private static long sleepSecond = 2;
	
	private CloudantSourceTaskConfig config;
	
	protected JavaCloudantOneTimeFeed feed = null;


	@Override
	public List<SourceRecord> poll() throws InterruptedException {
	
		String url = config.getString(InterfaceConst.URL);
		String userName = config.getString(InterfaceConst.USER_NAME);
		String password = config.getString(InterfaceConst.PASSWORD);
		List<String> topics = config.getList(InterfaceConst.TOPIC);
		
		String latestSequenceNumber = config.getString(InterfaceConst.LAST_CHANGE_SEQ);
		
		ArrayList<SourceRecord> records = new ArrayList<SourceRecord>();
		OffsetStorageReader offsetReader = context.offsetStorageReader();
		
		if (offsetReader != null) {
			Map<String, Object> offset = offsetReader.offset(Collections.singletonMap(InterfaceConst.URL, url));
			if (offset != null) {
				latestSequenceNumber = (String) offset.get(InterfaceConst.LAST_CHANGE_SEQ);
				LOG.debug("*** offset *** " + latestSequenceNumber);
			}
		}
	
	    feed = new JavaCloudantOneTimeFeed(url, userName, password, latestSequenceNumber);
	
		//add the first unit of work
		uowManager.addUnitOfWork(CLOUDANT_BATCH_SIZE);

		JSONObject change;
		JSONObject document;
		
		try{
			while (true) {
				change = feed.getNextChange();	
				if (change == null) {
					try {
						// Make the latest unit of work complete before going to sleep
						if (docCounter != CLOUDANT_BATCH_SIZE)
						{
							uowManager.setLastUnitOfWorkInfo(docCounter % CLOUDANT_BATCH_SIZE, latestSequenceNumber);
							docCounter = 0;
						}
						Thread.sleep(TimeUnit.SECONDS.toMillis(sleepSecond));
					} catch (InterruptedException e) {
						LOG.error("[" + url + "]" + e);
					}
			
					return records;
				}
				document = (JSONObject) change.get(CloudantConst.CLOUDANT_DOC);
				
				String sequenceNumber = JsonUtil.getStringValue(change, CloudantConst.SEQ);
				
				// Wrap the result
				JSONObject record = new JSONObject();
				record.put(CloudantConst.CLOUDANT_DOC, document);
				record = document;
				
				latestSequenceNumber = sequenceNumber;

				docCounter++;
					
				if (docCounter == CLOUDANT_BATCH_SIZE)
				{
					// For the last operations in an unit of work, we need to store its sequence number
					// Currently, the latestSequenceNumber is the previous record's seq
					uowManager.setLastOperationSequenceNumber(latestSequenceNumber);
					
					// Add a new unit of work.  All subsequent operations should belong to
					// this unit of work until a new unit of work is created.
					uowManager.addUnitOfWork(CLOUDANT_BATCH_SIZE);
					docCounter = 0;
				}
				
				// Emit the record to every topic configured
				for (String topic : topics) {
					records.add(new SourceRecord(offsetKey(url), offsetValue(latestSequenceNumber), topic, Schema.STRING_SCHEMA, document.toString()));
				}
				
				LOG.info("Last sequencce " + latestSequenceNumber);
			}
		} catch(Exception e){
			LOG.error(e.getMessage(), e);
		}
		
		LOG.info("Records array contains " + records.size() + " source records");
		return records;
	}

	@Override
	public void start(Map<String, String> props) {

		try {
			config = new CloudantSourceTaskConfig(props);
		} catch (ConfigException e) {
			throw new ConnectException(ResourceBundleUtil.get(MessageKey.CONFIGURATION_EXCEPTION), e);
		}

	    uowManager = new UnitOfWorkManager();
	}

	@Override
	public void stop() {
		// reader.finish();
	}
	

    private Map<String, String> offsetKey(String url) {
        return Collections.singletonMap(InterfaceConst.URL, url);
    }

    private Map<String, String> offsetValue(String lastSeqNumber) {
        return Collections.singletonMap(InterfaceConst.LAST_CHANGE_SEQ, lastSeqNumber);
    }

	public String version() {
		 return new CloudantSourceConnector().version();
	}
	
}
