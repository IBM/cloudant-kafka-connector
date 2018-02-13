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
import org.apache.kafka.connect.source.SourceRecord;
import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.powermock.api.easymock.PowerMock;

import java.io.FileReader;
import java.util.List;
import java.util.Map;

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

    public void testStructMessage() throws InterruptedException {
        PowerMock.replayAll();

        // Use the struct message format
        sourceProperties.put(InterfaceConst.USE_VALUE_SCHEMA_STRUCT, "true");

        // Run the task and process all documents currently in the _changes feed
        task.start(sourceProperties);
        List<SourceRecord> records = task.poll();
        assertTrue(records.size() > 0);
        assertEquals(999, records.size());

        // Inspect the first record and make sure it is a struct
        SourceRecord firstRecord = records.get(0);

        assertEquals("The key schema should be a string", Schema.STRING_SCHEMA, firstRecord.keySchema());

        Schema schema = firstRecord.valueSchema();

        // The default is a String schema, so it should not be that with the option enabled
        assertEquals("The value schema type should be a struct schema", Schema.Type.STRUCT, schema.type());
        assertTrue("There should be multiple fields in the schema", schema.fields().size() > 1);
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
