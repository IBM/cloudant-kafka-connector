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
import java.util.List;

import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.source.SourceRecord;

import com.cloudant.client.api.Database;
import com.ibm.cloudant.kafka.common.InterfaceConst;
import com.ibm.cloudant.kafka.common.utils.JavaCloudantUtil;

import junit.framework.TestCase;

/**
 * @author holger
 *
 */
public class CloudantSourceAndSinkTest extends TestCase {

	CloudantSourceTaskTest sourceTask;
	CloudantSinkTaskTest sinkTask;
	
	Database sourceDb;
	Database targetDb;
	
	/* (non-Javadoc)
	 * @see junit.framework.TestCase#setUp()
	 */
	protected void setUp() throws Exception {
		super.setUp();
		
		sourceTask = new CloudantSourceTaskTest();
		sinkTask = new CloudantSinkTaskTest();
		
		sourceTask.setUp();
		sinkTask.setUp();
		
		
		// Get the source database handle
		sourceDb = JavaCloudantUtil.getDBInst(
				sourceTask.getSourceProperties().get(InterfaceConst.URL), 
				sourceTask.getSourceProperties().get(InterfaceConst.USER_NAME),
				sourceTask.getSourceProperties().get(InterfaceConst.PASSWORD));
		
		// Create a _target database to replicate data into
		sinkTask.getTargetProperties().put(InterfaceConst.URL, 
				sinkTask.getTargetProperties().get(InterfaceConst.URL) + "_target");
		
		targetDb = JavaCloudantUtil.getDBInst(
				sinkTask.getTargetProperties().get(InterfaceConst.URL), 
				sinkTask.getTargetProperties().get(InterfaceConst.USER_NAME),
				sinkTask.getTargetProperties().get(InterfaceConst.PASSWORD));
	}

	public void testReplicateAll() {

		try {
			
			// 1. Trigger sourceTask to get a batch of records
			sourceTask.getTask().start(sourceTask.getSourceProperties());
			List<SourceRecord> records = new ArrayList<SourceRecord>();
			
			// 2. Trigger sinkTask
			sinkTask.getTask().start(sinkTask.getTargetProperties());
			List<SinkRecord> sinkRecords = new ArrayList<SinkRecord>();
		
			do {

				// 3a. Get a batch of source records
				records = sourceTask.getTask().poll();

				// Process every source record into a corresponding sink record
				// - 1 partition
				// - no schema
				// - no offset
				for (SourceRecord record : records) {

					SinkRecord sinkRecord = new SinkRecord(sinkTask.getTargetProperties().get(InterfaceConst.TOPIC), 
							0, // partition
							record.keySchema(), // key schema
							record.key(), // key
							record.valueSchema(), // value schema
							record.value(),  // value
							0); // offset
					sinkRecords.add(sinkRecord);
				}

				// 3b. Put all sinkRecords to the sink task
				sinkTask.getTask().put(sinkRecords);
				sinkRecords.clear();

			} while (records.size() > 0);

			// 4. Flush the latest set of records in case the batch has not been committed
			sinkTask.getTask().flush(null);
			
			// 5. Stop source and target task
			sourceTask.getTask().stop();
			sinkTask.getTask().stop();
			
			// 5. Compare the number of documents in the source and target dbs
			long sourceDocCount = sourceDb.info().getDocCount();
			long targetDocCount = targetDb.info().getDocCount();

			assertTrue(sourceDocCount > 0);
			assertTrue(targetDocCount > 0);

			assertEquals(sourceDocCount, targetDocCount);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	protected void tearDown() throws Exception {

		sourceTask.tearDown();
		sinkTask.tearDown();
	}
}
