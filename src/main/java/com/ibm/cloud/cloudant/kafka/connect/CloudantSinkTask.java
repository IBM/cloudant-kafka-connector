/*
 * Copyright © 2016, 2022 IBM Corp. All rights reserved.
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
package com.ibm.cloud.cloudant.kafka.connect;

import com.ibm.cloud.cloudant.kafka.common.InterfaceConst;
import com.ibm.cloud.cloudant.kafka.common.MessageKey;
import com.ibm.cloud.cloudant.kafka.common.utils.JavaCloudantUtil;
import com.ibm.cloud.cloudant.kafka.common.utils.ResourceBundleUtil;
import com.ibm.cloud.cloudant.kafka.schema.ConnectRecordMapper;
import com.ibm.cloud.cloudant.v1.model.DocumentResult;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class CloudantSinkTask extends SinkTask {

	private static Logger LOG = LoggerFactory.getLogger(CloudantSinkTask.class);
	
	private CloudantSinkTaskConfig config;
	
	List<String> topics = null;

	public static int batch_size = 0;
	private int taskNumber;
	public static String guid_schema = null;
	private Boolean replication;


	private static ConnectRecordMapper<SinkRecord> mapper = new ConnectRecordMapper<>();

	private ErrantRecordReporter reporter;

	// will be constructed on-demand
	private List<SinkRecord> accumulatedSinkRecords = null;

	@Override
	public String version() {
		 return new CloudantSinkConnector().version();
	}

	//TODO: all sinkRecords in first Thread
	@Override
	public void put(Collection<SinkRecord> sinkRecords) {

		if (accumulatedSinkRecords == null) {
			accumulatedSinkRecords = new LinkedList<>();
		}

		System.out.println(Arrays.toString(Thread.currentThread().getStackTrace()));

		LOG.info("Thread[" + Thread.currentThread().getId() + "].sinkRecords = " + sinkRecords.size());

		accumulatedSinkRecords.addAll(sinkRecords);
	}

	@Override
	public void stop() {
		// reader.finish();
	}

	/**
    * Start the Task. Handles configuration parsing and one-time setup of the task.
    *
    * @param props initial configuration
    */
	@Override
	public void start(Map<String, String> props) {

		reporter = context.errantRecordReporter();

 		try {
			config = new CloudantSinkTaskConfig(props);
            taskNumber = config.getInt(InterfaceConst.TASK_NUMBER);

            //TODO: split topics from Connector
            topics = config.getList(InterfaceConst.TOPIC);
            
            batch_size = config.getInt(InterfaceConst.BATCH_SIZE)==null ? InterfaceConst.DEFAULT_BATCH_SIZE : config.getInt(InterfaceConst.BATCH_SIZE);			
			replication = config.getBoolean(InterfaceConst.REPLICATION) == null ? InterfaceConst.DEFAULT_REPLICATION : config.getBoolean(InterfaceConst.REPLICATION); 
		} catch (ConfigException e) {
			throw new ConnectException(ResourceBundleUtil.get(MessageKey.CONFIGURATION_EXCEPTION), e);
		}
	}

	@Override
	public void flush(Map<TopicPartition, org.apache.kafka.clients.consumer.OffsetAndMetadata> offsets) {

		System.out.println(Arrays.toString(Thread.currentThread().getStackTrace()));

		List<Map<String, Object>> jsonArray = new LinkedList<>();

		if (accumulatedSinkRecords != null) {
			// Note: _rev is preserved
			accumulatedSinkRecords.stream()
					.map(mapper) // Convert ConnectRecord to Map
					.sequential() // Avoid concurrent access to jsonArray
					.forEach(recordValueAsMap -> {
						jsonArray.add(recordValueAsMap);
					});
		}


		try {
			LOG.info(String.format("Calling batchWrite with %d documents to %s", jsonArray.size(), config.getString(InterfaceConst.URL)));
			List<DocumentResult> writeResults = JavaCloudantUtil.batchWrite(config.originalsStrings(), jsonArray);
			boolean ok = writeResults.stream().allMatch(DocumentResult::isOk);
			if (!ok) {
				LOG.error("Failed to write some documents");
				for (int i=0; i<writeResults.size(); i++) {
					// TODO - is checking isOk sufficient?
					// TODO - are bulk doc results guaranteed to be in same order?
					// TODO - think of a better exception to raise here
					if (!writeResults.get(i).isOk()) {
						reporter.report(accumulatedSinkRecords.get(i), new RuntimeException("not ok"));
					}
				}
			}
		} catch (RuntimeException re) {
			LOG.error(String.format("Exception thrown when trying to write documents: %s", re.getMessage()));
			if (reporter != null) {
				if (accumulatedSinkRecords != null) {
					// all failed
					for (SinkRecord r : accumulatedSinkRecords) {
						reporter.report(r, re);
					}
				}
			} else {
				throw new ConnectException("Exception thrown when trying to write documents", re);
			}
		} finally {
			accumulatedSinkRecords = null;
		}
	}
	
	@Override
	public void open(Collection<TopicPartition> partitions) {
		LOG.info("Committed ");
		TopicPartition partition = new TopicPartition(topics.get(taskNumber), taskNumber);		
		partitions.add(partition);
	}
	
	@Override
	public void close(Collection<TopicPartition> partitions) {
		LOG.info("Committed ");
	}

	// for testing
	// TODO find out if there is a better way to do this without changing production classes
	public void setContext(SinkTaskContext ctx) {
		this.context = ctx;
	}

}
