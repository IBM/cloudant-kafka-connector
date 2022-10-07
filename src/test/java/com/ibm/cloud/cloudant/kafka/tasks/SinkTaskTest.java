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

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.ibm.cloud.cloudant.kafka.caching.CachedClientManager;
import com.ibm.cloud.cloudant.kafka.utils.InterfaceConst;
import com.ibm.cloud.cloudant.kafka.utils.JavaCloudantUtil;
import com.ibm.cloud.cloudant.kafka.utils.CloudantDbUtils;
import com.ibm.cloud.cloudant.kafka.utils.ConnectorUtils;
import com.ibm.cloud.cloudant.v1.Cloudant;
import com.ibm.cloud.cloudant.v1.model.ChangesResult;
import com.ibm.cloud.cloudant.v1.model.ChangesResultItem;
import com.ibm.cloud.cloudant.v1.model.Document;
import com.ibm.cloud.cloudant.v1.model.GetDatabaseInformationOptions;
import com.ibm.cloud.cloudant.v1.model.PostChangesOptions;
import junit.framework.TestCase;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author holger
 */
public class SinkTaskTest extends TestCase {

    private SinkTask task;
    private Map<String, String> targetProperties;

    private Map<String, Object> doc1, doc2, doc3;
    private Cloudant service;

    /* (non-Javadoc)
     * @see junit.framework.TestCase#setUp()
     */
    protected void setUp() throws Exception {
        super.setUp();

        targetProperties = ConnectorUtils.getTestProperties();

        task = new SinkTask();

        //Test objects

        doc1 = new HashMap<>();
        doc1.put("_id", "doc1");
        doc1.put("number", 1L);
        doc1.put("key", "value1");

        doc2 = new HashMap<>();
        doc2.put("_id", "doc2");
        doc2.put("number", 2L);
        doc2.put("key", "value2");

        doc3 = new HashMap<>();
        doc3.put("_id", "doc3");
        doc3.put("number", 3L);
        doc3.put("key", "value3");

        service = CachedClientManager.getInstance(targetProperties);
        JavaCloudantUtil.createTargetDb(service, targetProperties.get(InterfaceConst.DB));
    }

    /**
     * Test method for
     * {@link SinkTask#put(java.util.Collection)}.
     */
    public void testReplicateSinkRecordSchema() {
        List<JsonObject> result = testPutCollectionOfSinkRecord();

        Gson gson = new Gson();

        JsonObject doc1Expected = (JsonObject) gson.toJsonTree(doc1);
        JsonObject doc2Expected = (JsonObject) gson.toJsonTree(doc2);
        JsonObject doc3Expected = (JsonObject) gson.toJsonTree(doc3);

        //Test results
        assertEquals(3, result.size());
        assertTrue(result.contains(doc1Expected));
        assertTrue(result.contains(doc2Expected));
        assertTrue(result.contains(doc3Expected));
    }

    private List<JsonObject> testPutCollectionOfSinkRecord() {

        // Get the current update sequence
        String dbName = targetProperties.get(InterfaceConst.DB);
        GetDatabaseInformationOptions dbOptions =
                new GetDatabaseInformationOptions.Builder()
                        .db(dbName)
                        .build();
        String since = service.getDatabaseInformation(dbOptions).execute().getResult().getUpdateSeq(); // latest seq

        // KAFKA
        HashMap<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();

        // Emit 3 new documents
        task.start(targetProperties);

        task.put(Collections.singletonList(
                new SinkRecord(targetProperties.get(InterfaceConst.TOPIC), 0,
                        null, null, SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA), doc1, (long) doc1.get("number"))));

        task.flush(offsets);

        task.put(Arrays.asList(
                new SinkRecord(targetProperties.get(InterfaceConst.TOPIC),
                        0, null, null, SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA), doc2, (long) doc2.get("number")),

                new SinkRecord(targetProperties.get(InterfaceConst.TOPIC),
                        0, null, null, SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA), doc3, (long) doc3.get("number"))
        ));

        task.flush(offsets);

        // CLOUDANT
        PostChangesOptions options = new PostChangesOptions.Builder()
                .db(targetProperties.get(InterfaceConst.DB))
                .since(since)
                .limit(4)
                .includeDocs(true)
                .build();
        ChangesResult changesResult = service.postChanges(options).execute().getResult();

        //process the ChangesResult
        List<JsonObject> result = new ArrayList<>();
        for (ChangesResultItem row : changesResult.getResults()) {
            Document doc = row.getDoc();
            doc.setRev(null);
            result.add(new Gson().fromJson(doc.toString(), JsonObject.class));
        }

        return result;
    }

    /**
     * @return the targetProperties
     */
    public Map<String, String> getTargetProperties() {
        return targetProperties;
    }

    /**
     * @return the task
     */
    public SinkTask getTask() {
        return task;
    }

    protected void tearDown() throws Exception {

        // Remove the created database
        CloudantDbUtils.dropDatabase(targetProperties);

        super.tearDown();
    }
}
