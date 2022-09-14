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

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class CloudantSinkTask extends SinkTask {

	private static Logger LOG = LoggerFactory.getLogger(CloudantSinkTask.class);
	
	private CloudantSinkTaskConfig config;
	
	List<String> topics = null;

	public static int batchSize = 0;
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

	@Override
	public void put(Collection<SinkRecord> sinkRecords) {
		if (accumulatedSinkRecords == null) {
			accumulatedSinkRecords = new LinkedList<>();
		}
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

 		try {
			config = new CloudantSinkTaskConfig(props);
            taskNumber = config.getInt(InterfaceConst.TASK_NUMBER);

            //TODO: split topics from Connector
            topics = config.getList(InterfaceConst.TOPIC);
            
            batchSize = config.getInt(InterfaceConst.BATCH_SIZE)==null ? InterfaceConst.DEFAULT_BATCH_SIZE : config.getInt(InterfaceConst.BATCH_SIZE);
			replication = config.getBoolean(InterfaceConst.REPLICATION) == null ? InterfaceConst.DEFAULT_REPLICATION : config.getBoolean(InterfaceConst.REPLICATION); 
		} catch (ConfigException e) {
			throw new ConnectException(ResourceBundleUtil.get(MessageKey.CONFIGURATION_EXCEPTION), e);
		}
	}

	@Override
	public void flush(Map<TopicPartition, org.apache.kafka.clients.consumer.OffsetAndMetadata> offsets) {

		List<Map<String, Object>> jsonArray = new ArrayList<>();

		if (accumulatedSinkRecords != null && !accumulatedSinkRecords.isEmpty()) {
			LOG.info(String.format("flush called with %d documents to %s", accumulatedSinkRecords.size(), config.getString(InterfaceConst.URL)));
			// Note: _rev is preserved
			accumulatedSinkRecords.stream()
					.map(mapper) // Convert ConnectRecord to Map
					.sequential() // Avoid concurrent access to jsonArray
					.forEach(recordValueAsMap -> {
						jsonArray.add(recordValueAsMap);
					});
			try {
				// break down accumulated records into batches to send to cloudant
				// NB a failure in _any_ batch will currently cause all accumulated records to be marked as uncommitted
				int nBatches = jsonArray.size() / batchSize + (jsonArray.size() % batchSize == 0 ? 0 : 1);
				LOG.info(String.format("flush called with %d batches to %s", nBatches, config.getString(InterfaceConst.URL)));
				for (int b = 0; b < nBatches; b++) {
					List<Map<String, Object>> jsonSubArray = jsonArray.subList(b * batchSize, Math.min(jsonArray.size(), (b + 1) * batchSize));
					LOG.info(String.format("Calling batchWrite with %d documents to %s", jsonSubArray.size(), config.getString(InterfaceConst.URL)));
					List<DocumentResult> writeResults = JavaCloudantUtil.batchWrite(config.originalsStrings(), jsonSubArray);
					boolean someFailed = writeResults.stream().anyMatch(doc -> doc.isOk() == null || !doc.isOk());
					if (reporter != null && someFailed) {
						// logging not needed - user can enable `errors.log.enable` if required
						for (int i = 0; i < writeResults.size(); i++) {
							DocumentResult writeResult = writeResults.get(i);
							if (writeResult.isOk() == null || !writeResult.isOk()) {
								reporter.report(accumulatedSinkRecords.get(b * batchSize + i),
										new RuntimeException(String.format("Failed to batch write document to Cloudant with error %s reason %s",
												writeResult.getError(), writeResult.getReason())));
							}
						}
					}
				}

				// if we got here, then the batch operation succeeded (ie no network failure and 2xx response)
				// therefore we can clear out the accumulated sink records
				// any individual failures reported back from the response to batch write will have been reported
				// and potentially logged and/or sent to dlq if the user configured these
				accumulatedSinkRecords = null;
			} catch (RuntimeException re) {
				// below we re-wrap the exception as a nicety - it's not required, see explanation below

				// WorkerSinkTask#commitOffsets will catch any Throwable and will not advance the offsets, meaning
				// that everything outstanding in accumulatedSinkRecords (and potentially more if put is called again)
				// will be attempted to be re-written

				// logging not needed - WorkerSinkTask#onCommitCompleted will log error including the below message
				throw new ConnectException("Exception thrown when trying to write documents", re);
			}
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

	@Override
	public void initialize(SinkTaskContext context) {
		super.initialize(context);
		this.reporter = context.errantRecordReporter();
	}
}
