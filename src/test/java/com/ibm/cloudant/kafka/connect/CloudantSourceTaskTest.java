/*
 * Copyright © 2016, 2018 IBM Corp. All rights reserved.
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

import com.ibm.cloudant.kafka.common.CloudantConst;
import com.ibm.cloudant.kafka.common.InterfaceConst;
import com.ibm.cloudant.kafka.common.utils.JavaCloudantUtil;
import com.ibm.cloudant.kafka.connect.utils.CloudantDbUtils;
import com.ibm.cloudant.kafka.connect.utils.ConnectorUtils;

import junit.framework.TestCase;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.source.SourceRecord;
import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.powermock.api.easymock.PowerMock;

import java.io.FileReader;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class CloudantSourceTaskTest extends TestCase {

    private CloudantSourceTask task;
    private Map<String, String> sourceProperties;

    private JSONArray data = null;

    protected void setUp() throws Exception {

        super.setUp();

        sourceProperties = ConnectorUtils.getSourceProperties();

        /*
         * 1. Create a database and load data
         */
        JSONTokener tokener = new JSONTokener(new FileReader("src/test/resources/data.json"));
        data = new JSONArray(tokener);

        // Load data into the source database (create if it does not exist)
        JavaCloudantUtil.batchWrite(sourceProperties.get(InterfaceConst.URL),
                sourceProperties.get(InterfaceConst.USER_NAME),
                sourceProperties.get(InterfaceConst.PASSWORD),
                data);

        /*
         * 2. Create connector
         */
        task = ConnectorUtils.createCloudantSourceConnector(sourceProperties);
    }

    public void testStartMapOfStringString() throws InterruptedException {
        PowerMock.replayAll();

        // Run the task and process all documents currently in the _changes feed
        task.start(sourceProperties);
        List<SourceRecord> records = task.poll();
        assertTrue(records.size() > 0);
        assertEquals(999, records.size());

        // Inspect the first record and make sure it is a valid Cloudant doc
        SourceRecord firstRecord = records.get(0);
        String firstValue = firstRecord.value().toString();

        JSONTokener tokener = new JSONTokener(firstValue);

        JSONObject firstObject = new JSONObject(tokener);
        assertNotNull(firstObject);

        assertNotNull(firstObject.get(CloudantConst.CLOUDANT_DOC_ID));
        assertNotNull(firstObject.get(CloudantConst.CLOUDANT_REV));

    }

    public void testStartWithIncrementalUpdates() throws InterruptedException {

        PowerMock.replayAll();

        // Test again with a last sequence number 0 and a batch size > number of documents
        sourceProperties.put(InterfaceConst.LAST_CHANGE_SEQ, "0");
        sourceProperties.put(InterfaceConst.BATCH_SIZE, "1000");

        task.start(sourceProperties);
        List<SourceRecord> records2 = task.poll();

        // We have 999 docs in the database at this point
        assertEquals(999, records2.size());

        // Load 20 new documents
        JSONArray data2 = new JSONArray();
        int new_changes = 20;
        for (int i = 0; i < new_changes; i++) {
            data2.put(data.get(i));
        }

        JavaCloudantUtil.batchWrite(sourceProperties.get(InterfaceConst.URL),
                sourceProperties.get(InterfaceConst.USER_NAME),
                sourceProperties.get(InterfaceConst.PASSWORD),
                data2);

        // Poll again for changes and expect to get the 20 we just inserted
        // (even though database has 999 + 20 documents now)
        records2 = task.poll();
        assertEquals(new_changes, records2.size());
    }

    public void testStructMessage() throws Exception {
        PowerMock.replayAll();

        // Use the struct message format
        sourceProperties.put(InterfaceConst.USE_VALUE_SCHEMA_STRUCT, "true");

        runAndAssertDocStructField(false);
    }

    public void testFlattenedStructMessage() throws Exception {
        PowerMock.replayAll();

        // Use the struct message format
        sourceProperties.put(InterfaceConst.USE_VALUE_SCHEMA_STRUCT, "true");
        // with flattening
        sourceProperties.put(InterfaceConst.FLATTEN_VALUE_SCHEMA_STRUCT, "true");

        runAndAssertDocStructField(true);
    }

    public void testOmitDesignDoc() throws Exception {
        // Add an additional doc, specifically a design doc
        JSONArray ddocArray = new JSONArray();
        ddocArray.put(Collections.singletonMap("_id", "_design/test"));
        JavaCloudantUtil.batchWrite(sourceProperties.get(InterfaceConst.URL),
                sourceProperties.get(InterfaceConst.USER_NAME),
                sourceProperties.get(InterfaceConst.PASSWORD),ddocArray);

        PowerMock.replayAll();

        // Omit design docs
        sourceProperties.put(InterfaceConst.OMIT_DESIGN_DOCS, "true");

        task.start(sourceProperties);
        List<SourceRecord> records = task.poll();

        // We have 1000 docs in the database at this point, but 1 is the design doc which should
        // be omitted so assert on 999.
        assertEquals(999, records.size());

    }

    private void runAndAssertDocStructField(boolean isFlattened) throws Exception {
        // Run the task and process all documents currently in the _changes feed
        task.start(sourceProperties);
        List<SourceRecord> records = task.poll();
        assertTrue(records.size() > 0);
        assertEquals(999, records.size());

        // Inspect the first record and make sure it is a struct
        for (int i = 0; i <= 2; i++) {
            SourceRecord record = records.get(i);

            assertEquals("The key schema should be a string", Schema.STRING_SCHEMA, record
                    .keySchema());

            Schema schema = record.valueSchema();

            // The default is a String schema, so it should not be that with the option enabled
            assertEquals("The value schema type should be a struct schema", Schema.Type.STRUCT,
                    schema.type());

            if (isFlattened) {
                // The exact record we receive first is undefined, so we don't know how mnany
                // fields there will be, but we can check we have more fields than in the
                // non-flattened case
                assertTrue("There should be more fields than an unflattened doc", schema
                        .fields().size() > 6);
            } else {
                assertEquals("There should be the correct number of fields in the schema", 6, schema
                        .fields().size());
            }

            // The exact record we receive is undefined, but we can check for a non-null value
            Struct s = (Struct) record.value();
            assertNotNull("The struct should not be null", s);
            try {
                if (isFlattened) {
                    assertNotNull("A value should be present", s.getString("doc.message.body"));
                } else {
                    assertNotNull("A value should be present", s.getStruct("doc").getStruct("message")
                            .getString("body"));
                }
                break;
            } catch(DataException e) {
                // There are two documents in the dataset that don't have a message, since the order
                // is undefined we might get one of these and end up here, if we do we should try
                // again with the next record.
                continue;
            }
        }
    }

    public void testMultipleConnectorInstances() throws Exception {

        ExecutorService e = Executors.newFixedThreadPool(2);

        // Second source properties
        Map<String, String> sourceProps2 = ConnectorUtils.getSourceProperties();

        try {
            // Create a second database with different content to the first
            JSONArray data2 = new JSONArray(new JSONTokener(new FileReader
                    ("src/test/resources/data2.json")));
            // Load data into the source database (create if it does not exist)
            JavaCloudantUtil.batchWrite(sourceProps2.get(InterfaceConst.URL),
                    sourceProps2.get(InterfaceConst.USER_NAME),
                    sourceProps2.get(InterfaceConst.PASSWORD),
                    data2);

            // Create second connector
            CloudantSourceTask task2 = ConnectorUtils.createCloudantSourceConnector(sourceProps2);

            Future<List<SourceRecord>> fr1 = e.submit(new TaskCallable(task, sourceProperties));
            Future<List<SourceRecord>> fr2 = e.submit(new TaskCallable(task2, sourceProps2));

            // Wait up to 5 minutes for each of these
            List<SourceRecord> r1 = fr1.get(5, TimeUnit.MINUTES);
            List<SourceRecord> r2 = fr2.get(5, TimeUnit.MINUTES);

            assertEquals(999, r1.size());
            assertEquals(1639, r2.size());

        } finally {
            // Delete the second database
            CloudantDbUtils.dropDatabase(sourceProps2.get(InterfaceConst.URL),
                    sourceProps2.get(InterfaceConst.USER_NAME),
                    sourceProps2.get(InterfaceConst.PASSWORD));
            e.shutdown();
        }
    }

    private static final class TaskCallable implements Callable<List<SourceRecord>> {

        final CloudantSourceTask t;
        final Map<String, String> config;

        TaskCallable(CloudantSourceTask t, Map<String, String> config) {
            this.t = t;
            this.config = config;
        }

        @Override
        public List<SourceRecord> call() throws Exception {
            t.start(config);
            return t.poll();
        }
    }

    /**
     * @return the sourceProperties
     */
    public Map<String, String> getSourceProperties() {
        return sourceProperties;
    }

    /**
     * @return the task
     */
    public CloudantSourceTask getTask() {
        return task;
    }

    protected void tearDown() throws Exception {

        // Remove the created database
        CloudantDbUtils.dropDatabase(sourceProperties.get(InterfaceConst.URL),
                sourceProperties.get(InterfaceConst.USER_NAME),
                sourceProperties.get(InterfaceConst.PASSWORD));

        super.tearDown();
    }

}
