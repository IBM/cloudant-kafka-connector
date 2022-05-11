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
package com.ibm.cloudant.kafka.connect;

import com.ibm.cloud.cloudant.v1.Cloudant;
import com.ibm.cloudant.kafka.common.CloudantConst;
import com.ibm.cloudant.kafka.common.InterfaceConst;
import com.ibm.cloudant.kafka.common.MessageKey;
import com.ibm.cloudant.kafka.common.utils.JavaCloudantUtil;
import com.ibm.cloudant.kafka.common.utils.ResourceBundleUtil;
import com.ibm.cloudant.kafka.schema.ConnectRecordMapper;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static com.ibm.cloudant.kafka.common.utils.JavaCloudantUtil.getClientInstance;


public class CloudantSinkTask extends SinkTask {

	private static Logger LOG = LoggerFactory.getLogger(CloudantSinkTask.class);
	
	private CloudantSinkTaskConfig config;

	private Cloudant service;
	
	List<String> topics = null;

	public static int batch_size = 0;
	private int taskNumber;
	public static String guid_schema = null;
	private Boolean replication;

	private List<Map<String, Object>> jsonArray = new LinkedList<>();

	private static ConnectRecordMapper<SinkRecord> mapper = new ConnectRecordMapper<>();
	
	@Override
	public String version() {
		 return new CloudantSinkConnector().version();
	}

	//TODO: all sinkRecords in first Thread
	@Override
	public void put(Collection<SinkRecord> sinkRecords) {

		LOG.info("Thread[" + Thread.currentThread().getId() + "].sinkRecords = " + sinkRecords.size());

		sinkRecords.stream()
				.map(mapper) // Convert ConnectRecord to Map
				.sequential() // Avoid concurrent access to jsonArray
				.forEach(recordValueAsMap -> {
					recordValueAsMap.remove(CloudantConst.CLOUDANT_REV); // Remove the _rev
					jsonArray.add(recordValueAsMap);
					if (jsonArray.size() >= batch_size) {
						flush(null);
					}
				});
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
 		
 		try {
			config = new CloudantSinkTaskConfig(props);
            taskNumber = config.getInt(InterfaceConst.TASK_NUMBER);
			service = getClientInstance(props);

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
		LOG.debug("Flushing output stream for {" + config.getString(InterfaceConst.URL) + "}");
		try {
				JavaCloudantUtil.batchWrite(config.originalsStrings(), service, jsonArray);
				LOG.info("Committed " + jsonArray.size() + " documents to -> " + config.getString(InterfaceConst.URL));
		} finally {
			// Release memory (regardless if documents got committed or not)
			jsonArray = new LinkedList<>();
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

}
