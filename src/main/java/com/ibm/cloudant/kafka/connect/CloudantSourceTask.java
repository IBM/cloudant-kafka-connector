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
import java.util.ListIterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.apache.log4j.Logger;

import com.cloudant.client.api.ClientBuilder;
import com.cloudant.client.api.CloudantClient;
import com.cloudant.client.api.Database;
import com.cloudant.client.api.model.ChangesResult;
import com.cloudant.client.api.model.ChangesResult.Row;
import com.ibm.cloudant.kafka.common.CloudantConst;
import com.ibm.cloudant.kafka.common.InterfaceConst;
import com.ibm.cloudant.kafka.common.MessageKey;
import com.ibm.cloudant.kafka.common.utils.ResourceBundleUtil;

public class CloudantSourceTask extends SourceTask {
	
	private static Logger LOG = Logger.getLogger(CloudantSourceTask.class);

	private static long FEED_SLEEP_MILLISEC = 5000;
	private static long SHUTDOWN_DELAY_MILLISEC = 10;
	private static String DEFAULT_CLOUDANT_LAST_SEQ = "0";
	
	private AtomicBoolean stop;
	private AtomicBoolean _running;
	
	private CloudantSourceTaskConfig config;
	
	String url = null;
	String userName = null;
	String password = null;
	List<String> topics = null;
	
	private static String latestSequenceNumber = null;
	private static int batch_size = 0;

	int task_number = 0;
	int tasks_max = 0;

	private static CloudantClient cantClient = null;
	private ChangesResult cantChangeResult = null;
	private static Database cantDB = null; 
	private static String cantDBName = null;
	
	@Override
	public List<SourceRecord> poll() throws InterruptedException {
		
				
		// stop will be set but can be honored only after we release
		// the changes() feed
		while (!stop.get()) {
			_running.set(true);
			
			// the array to be returend
			ArrayList<SourceRecord> records = new ArrayList<SourceRecord>();

			LOG.debug("Process lastSeq: " + latestSequenceNumber);
			
			// the changes feed for initial processing (not continuous yet)
			cantChangeResult = cantDB.changes()
					.includeDocs(true)
					.since(latestSequenceNumber)
					.limit(batch_size)
					.getChanges();

			if (cantDB.changes() != null) {
				
				// This indicates that we have exhausted the initial feed
				// and can request a continuous feed next
				if (cantChangeResult.getResults().size() == 0) {

					LOG.debug("Get continuous feed for lastSeq: " + latestSequenceNumber);

					// the continuous changes feed
					cantChangeResult = cantDB.changes()
							.includeDocs(true)
							.since(latestSequenceNumber)
							.limit(batch_size)
							.continuousChanges()
							.heartBeat(FEED_SLEEP_MILLISEC)
							.getChanges();
				}


				LOG.debug("Got " + cantChangeResult.getResults().size() + " changes");
				latestSequenceNumber = cantChangeResult.getLastSeq();

				// process the results into the array to be returned
				for (ListIterator<Row> it = cantChangeResult.getResults().listIterator(); it.hasNext(); ) {
					Row row_ = it.next();

					// Emit the record to every topic configured
					for (String topic : topics) {
						SourceRecord sourceRecord = new SourceRecord(offsetKey(url), 
								offsetValue(latestSequenceNumber), topic,
								Schema.STRING_SCHEMA, // key schema
								row_.getId(), // key
								Schema.STRING_SCHEMA, // value schema
								row_.getDoc().toString()); // value
						records.add(sourceRecord);
					}
				}

				LOG.info("Return " + records.size() / topics.size() + " records with last offset " 
				+ latestSequenceNumber);

				cantChangeResult = null;
				
				_running.set(false);
				return records;
			}
		}

		// Only in case of shutdown
		return null;
	}	


	@Override
	public void start(Map<String, String> props) {

		try {
			config = new CloudantSourceTaskConfig(props);

			url = config.getString(InterfaceConst.URL);
			userName = config.getString(InterfaceConst.USER_NAME);
			password = config.getString(InterfaceConst.PASSWORD);
			topics = config.getList(InterfaceConst.TOPIC);

			latestSequenceNumber = config.getString(InterfaceConst.LAST_CHANGE_SEQ);
			task_number = config.getInt(InterfaceConst.TASK_NUMBER);
			tasks_max =  config.getInt(InterfaceConst.TASKS_MAX);			
			batch_size = config.getInt(InterfaceConst.BATCH_SIZE)==null ? CloudantConst.DEFAULT_BATCH_SIZE : config.getInt(InterfaceConst.BATCH_SIZE);
				
			if (tasks_max > 1) {
				throw new ConfigException("CouchDB _changes API only supports 1 thread. Configure tasks.max=1");
			}
			// Use like a semaphore to allow synchronization
			// between poll() and stop() methods
			_running = new AtomicBoolean(false);
			stop = new AtomicBoolean(false);
			
			if (latestSequenceNumber == null) {
				latestSequenceNumber = new String(DEFAULT_CLOUDANT_LAST_SEQ);
				
				OffsetStorageReader offsetReader = context.offsetStorageReader();

				if (offsetReader != null) {
					Map<String, Object> offset = offsetReader.offset(Collections.singletonMap(InterfaceConst.URL, url));
					if (offset != null) {
						latestSequenceNumber = (String) offset.get(InterfaceConst.LAST_CHANGE_SEQ);
						LOG.info("Start with current offset (last sequence): " + latestSequenceNumber);
					}
				}
			}

			// Create a new CloudantClient instance for account endpoint account.cloudant.com
			String urlWithoutProtocal = url.substring(url.indexOf("://") +3);
			String account = urlWithoutProtocal.substring(0,urlWithoutProtocal.indexOf("."));
			
			cantClient = ClientBuilder.account(account)
			                          .username(userName)
			                          .password(password)
			                          .build();
			
			// Create a database instance
			cantDBName = url.substring(url.lastIndexOf("/")+1);
			
			// Create a database instance
			cantDB = cantClient.database(cantDBName, false);
			
		} catch (ConfigException e) {
			throw new ConnectException(ResourceBundleUtil.get(MessageKey.CONFIGURATION_EXCEPTION), e);
		}

	}

	@Override
	public void stop() {
		if (stop != null) {
			stop.set(true);
		}
		
		// terminate the changes feed
		// Careful: this is an asynchronous call
		if (cantDB.changes() != null) {		
			cantDB.changes().stop();
			
			// graceful shutdown
			// allow the poll() method to flush records first
			if (_running == null) return;
			
			while (_running.get() == true) {
				try {
					Thread.sleep(SHUTDOWN_DELAY_MILLISEC);
				} catch (InterruptedException e) {
					LOG.error(e);
				}
			}
		}
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
