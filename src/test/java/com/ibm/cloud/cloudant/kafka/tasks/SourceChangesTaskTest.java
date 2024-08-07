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
package com.ibm.cloud.cloudant.kafka.tasks;

import java.io.Reader;
import java.lang.reflect.Type;
import java.util.ArrayList;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.ibm.cloud.cloudant.kafka.utils.CloudantConst;
import com.ibm.cloud.cloudant.kafka.utils.InterfaceConst;
import com.ibm.cloud.cloudant.kafka.utils.JavaCloudantUtil;
import com.ibm.cloud.cloudant.v1.model.Document;
import com.ibm.cloud.cloudant.kafka.utils.CloudantDbUtils;
import com.ibm.cloud.cloudant.kafka.utils.ConnectorUtils;
import junit.framework.TestCase;
import org.apache.kafka.connect.source.SourceRecord;
import org.powermock.api.easymock.PowerMock;
import java.io.FileReader;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

public class SourceChangesTaskTest extends TestCase {

    private static final Type documentListTypeToken = new TypeToken<List<Document>>() {}.getType();

    private SourceChangesTask task;

    private Map<String, String> sourceProperties;

    private List<Document> data = null;

    private Gson gson = new Gson();

    protected void setUp() throws Exception {

        super.setUp();

        sourceProperties = ConnectorUtils.getTestProperties();

        /*
         * 1. Create a database and load data
         */
        try (Reader reader = new FileReader("src/test/resources/data.json")) {
            data = gson.fromJson(reader, documentListTypeToken);
        }

        // Load data into the source database (create if it does not exist)
        JavaCloudantUtil.batchWrite(sourceProperties, data);

        /*
         * 2. Create connector
         */
        task = ConnectorUtils.createCloudantSourceConnector(sourceProperties);
    }

    @SuppressWarnings("unchecked")
    public void testStartMapOfStringString() throws InterruptedException {
        PowerMock.replayAll();

        // Run the task and process all documents currently in the _changes feed
        task.start(sourceProperties);
        List<SourceRecord> records = task.poll();
        assertTrue(records.size() > 0);
        assertEquals(999, records.size());

        // Inspect the first record and make sure it is a valid Cloudant doc
        SourceRecord firstRecord = records.get(0);
        Map<String, Object> firstValue = (Map<String, Object>) firstRecord.value();
        assertNotNull(firstValue);
        assertNotNull(firstValue.get(CloudantConst.CLOUDANT_DOC_ID));
        assertNotNull(firstValue.get(CloudantConst.CLOUDANT_REV));
    }

    public void testSourcePartitionAndOffsetMaps() throws InterruptedException {
        PowerMock.replayAll();

        // Run the task and process all documents currently in the _changes feed
        task.start(sourceProperties);
        List<SourceRecord> records = task.poll();
        assertTrue("There should be at least two records.", records.size() >= 2);

        // Get the first record
        SourceRecord firstRecord = records.get(0);
        assertNotNull(firstRecord);

        // Partition map
        Map<String, ?> sourcePartition = firstRecord.sourcePartition();
        assertNotNull("The source partition should not be null.", sourcePartition);
        assertEquals("There should be two entries in the map.", 2, sourcePartition.size());
        // URL entry
        assertEquals("The URL entry should match the config.", sourceProperties.get(InterfaceConst.URL), 
                sourcePartition.get(InterfaceConst.URL));
        // DB entry
        assertEquals("The DB entry should match the config.", sourceProperties.get(InterfaceConst.DB), 
                sourcePartition.get(InterfaceConst.DB));

        // Offset map
        Map<String, ?> sourceOffset = firstRecord.sourceOffset();
        assertNotNull("The offset should not be null.", sourceOffset);
        assertEquals("There should be one entry in the map.", 1, sourceOffset.size());
        Object sourceOffsetSeq = sourceOffset.get(InterfaceConst.LAST_CHANGE_SEQ);
        assertNotNull("The offset seq entry should not be null.", sourceOffsetSeq);
        String offsetSeq = String.valueOf(sourceOffsetSeq);
        assertTrue(String.format("The offset seq %s should match a seq pattern.", offsetSeq),
                Pattern.matches("\\d+-[A-Za-z0-9_-]+", offsetSeq));

        // Assert that first and second record partitions are the same, but offsets different
        // Get the second record
        SourceRecord secondRecord = records.get(1);
        assertNotNull(secondRecord);
        assertEquals("The first and second records should have the same source partition.",
                firstRecord.sourcePartition(),
                secondRecord.sourcePartition());
        assertFalse("The first and second records should have different offsets.",
                firstRecord.sourceOffset().equals(secondRecord.sourceOffset()));
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
        ArrayList<Document> data2 = new ArrayList<>();
        int new_changes = 20;
        for (int i = 0; i < new_changes; i++) {
            data2.add(data.get(i));
        }

        JavaCloudantUtil.batchWrite(sourceProperties, data2);

        // Poll again for changes and expect to get the 20 we just inserted
        // (even though database has 999 + 20 documents now)
        records2 = task.poll();
        assertEquals(new_changes, records2.size());
    }

    public void testMultipleConnectorInstances() throws Exception {

        ExecutorService e = Executors.newFixedThreadPool(2);

        // Second source properties
        Map<String, String> sourceProps2 = ConnectorUtils.getTestProperties();

        try {
            // Create a second database with different content to the first
            List<Document> data2;
            try (Reader reader = new FileReader("src/test/resources/data2.json")) {
                data2 = gson.fromJson(reader, documentListTypeToken);
            }

            // Load data into the source database (create if it does not exist)
            JavaCloudantUtil.batchWrite(sourceProps2, data2);

            // Create second connector
            SourceChangesTask task2 = ConnectorUtils.createCloudantSourceConnector(sourceProps2);

            Future<List<SourceRecord>> fr1 = e.submit(new TaskCallable(task, sourceProperties));
            Future<List<SourceRecord>> fr2 = e.submit(new TaskCallable(task2, sourceProps2));

            // Wait up to 5 minutes for each of these
            List<SourceRecord> r1 = fr1.get(5, TimeUnit.MINUTES);
            List<SourceRecord> r2 = fr2.get(5, TimeUnit.MINUTES);

            assertEquals(999, r1.size());
            assertEquals(1639, r2.size());

        } finally {
            // Delete the second database
            CloudantDbUtils.dropDatabase(sourceProps2);
            e.shutdown();
        }
    }

    private static final class TaskCallable implements Callable<List<SourceRecord>> {

        final SourceChangesTask t;
        final Map<String, String> config;

        TaskCallable(SourceChangesTask t, Map<String, String> config) {
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
    public SourceChangesTask getTask() {
        return task;
    }

    protected void tearDown() throws Exception {

        // Remove the created database
        CloudantDbUtils.dropDatabase(sourceProperties);

        super.tearDown();
    }

}
