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

import com.google.gson.Gson;
import com.ibm.cloud.cloudant.v1.Cloudant;
import com.ibm.cloud.cloudant.v1.model.GetDatabaseInformationOptions;
import com.ibm.cloudant.kafka.common.InterfaceConst;
import com.ibm.cloudant.kafka.common.utils.JavaCloudantUtil;

import junit.framework.TestCase;

import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author holger
 */
public class CloudantSourceAndSinkTest extends TestCase {

    private CloudantSourceTaskTest sourceTask;
    private CloudantSinkTaskTest sinkTask;

    private Cloudant sourceService;
    private Cloudant targetService;

    private String sourceDbName;
    private String targetDbName;

    private Gson gson = new Gson();

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
        sourceService = CachedClientManager.getInstance(sourceTask.getSourceProperties());

        sourceDbName = sourceTask.getSourceProperties().get(InterfaceConst.DB);

        // Create a _target database to replicate data into
        sinkTask.getTargetProperties().put(InterfaceConst.DB,
                sinkTask.getTargetProperties().get(InterfaceConst.DB) + "_target");
        targetDbName = sinkTask.getTargetProperties().get(InterfaceConst.DB);
        JavaCloudantUtil.createTargetDb(sourceService, targetDbName);

        targetService = CachedClientManager.getInstance(sinkTask.getTargetProperties());
    }

    public void testReplicateAll() throws Exception {

        // 1. Trigger sourceTask to get a batch of records
        sourceTask.getTask().start(sourceTask.getSourceProperties());
        List<SourceRecord> records;

        // 2. Trigger sinkTask
        sinkTask.getTask().start(sinkTask.getTargetProperties());
        List<SinkRecord> sinkRecords = new ArrayList<>();

        do {

            // 3a. Get a batch of source records
            records = sourceTask.getTask().poll();

            // Process every source record into a corresponding sink record
            // - 1 partition
            // - no schema
            // - no offset
            for (SourceRecord record : records) {

                // source task returns strings but sink task expects structs or maps
                // in a real kafka instance this would be fixed by using appropriate converters
                Map recordValue = gson.fromJson((String)record.value(), Map.class);
                recordValue.remove("_rev");

                SinkRecord sinkRecord = new SinkRecord(sinkTask.getTargetProperties().get
                        (InterfaceConst.TOPIC),
                        0, // partition
                        record.keySchema(), // key schema
                        record.key(), // key
                        null, // value schema
                        recordValue,  // value
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
        GetDatabaseInformationOptions sourceDbOptions = new GetDatabaseInformationOptions.Builder()
            .db(sourceDbName)
            .build();
        GetDatabaseInformationOptions targetDbOptions = new GetDatabaseInformationOptions.Builder()
            .db(targetDbName)
            .build();
        long sourceDocCount = sourceService.getDatabaseInformation(sourceDbOptions).execute().getResult().getDocCount();
        long targetDocCount = targetService.getDatabaseInformation(targetDbOptions).execute().getResult().getDocCount();

        assertTrue(sourceDocCount > 0);
        assertTrue(targetDocCount > 0);

        assertEquals(sourceDocCount, targetDocCount);
    }

    protected void tearDown() throws Exception {

        sourceTask.tearDown();
        sinkTask.tearDown();
        super.tearDown();
    }
}
