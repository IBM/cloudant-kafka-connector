/*
 * Copyright © 2017, 2022 IBM Corp. All rights reserved.
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
package com.ibm.cloud.cloudant.kafka.performance;

import com.carrotsearch.junitbenchmarks.AbstractBenchmark;
import com.carrotsearch.junitbenchmarks.BenchmarkOptions;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.ibm.cloud.cloudant.kafka.common.InterfaceConst;
import com.ibm.cloud.cloudant.kafka.connect.CachedClientManager;
import com.ibm.cloud.cloudant.kafka.connect.CloudantSinkTask;
import com.ibm.cloud.cloudant.kafka.connect.utils.CloudantDbUtils;
import com.ibm.cloud.cloudant.kafka.connect.utils.ConnectorUtils;
import com.ibm.cloud.cloudant.v1.Cloudant;
import com.ibm.cloud.cloudant.v1.model.DatabaseInformation;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CloudantSinkPerformanceTest extends AbstractBenchmark {
    private static Cloudant targetService;
    private static JsonObject testResults1 = new JsonObject();
    private static JsonObject testResults2 = new JsonObject();
    private static JsonObject testResults3 = new JsonObject();

    private Map<String, String> targetProperties;
    private CloudantSinkTask sinkTask;
    private List<SinkRecord> sinkRecords;

    @Before
    public void setUp() {
        //Set properties
        targetProperties = ConnectorUtils.getTestProperties();

        // Append _target to the performance URL DB and update to use that
        targetProperties.put(InterfaceConst.URL, targetProperties.get(ConnectorUtils
                .PERFORMANCE_URL) + "_target");

        // Create a _target database to replicate data into
        targetService = CachedClientManager.getInstance(targetProperties);

        //Create Connector
        sinkTask = new CloudantSinkTask();

        //Create Kafka SinkRecord
        sinkRecords = new ArrayList<>();
    }

    @BenchmarkOptions(benchmarkRounds = 3, warmupRounds = 0)
    @Test
    public void testSinkPerformance() {
        //set parameter => init(docsNumber, topic, batch.size, tasks.max, guid.schema)
        init(100000, 1, 10000, 1, false);
        long testTime = runTest();
        testResults1 = addResults(testResults1, testTime);
    }

    @BenchmarkOptions(benchmarkRounds = 3, warmupRounds = 0)
    @Test
    public void testSinkPerformance2() {
        //set parameter => init(docsNumber, topic, batch.size, tasks.max, guid.schema)
        init(100000, 1, 10000, 1, false);
        long testTime = runTest();
        testResults2 = addResults(testResults2, testTime);
    }

    @BenchmarkOptions(benchmarkRounds = 3, warmupRounds = 0)
    @Test
    public void testSinkPerformance3() {
        //set parameter => init(docsNumber, topic, batch.size, tasks.max, guid.schema)
        init(100000, 1, 10000, 1, false);
        long testTime = runTest();
        testResults3 = addResults(testResults3, testTime);
    }

    private void init(int numberDocs, int numTopics, int batch_size, int tasks_max, Boolean
            replication) {
        List<String> topics = new ArrayList<String>();
        JsonParser parser = new JsonParser();

        for (int i = 0; i < numTopics; i++) {
            topics.add("topic" + i);
        }

        for (String topic : topics) {
            for (int i = 0; i < numberDocs; i++) {
                JsonObject doc = parser.parse("{\"_id\":\"" + i + "\"}").getAsJsonObject();

                sinkRecords.add(new SinkRecord(topic, 0, null, null, Schema.STRING_SCHEMA, doc, i));
            }
        }
        targetProperties.put(InterfaceConst.TOPIC, topics.toString());
        targetProperties.put(InterfaceConst.BATCH_SIZE, Integer.toString(batch_size));
        targetProperties.put(InterfaceConst.TASKS_MAX, Integer.toString(tasks_max));
    }

    private JsonObject addResults(JsonObject results, long testTime) {
        if (results.size() == 0) {
            JsonArray testTimes = new JsonArray();
            testTimes.add(testTime);
            results.addProperty("testRounds", 1);
            DatabaseInformation dbInfo = CloudantDbUtils.getDbInfo(
                    targetProperties.get(InterfaceConst.DB), targetService);
            results.addProperty("diskSize", dbInfo.getSizes().getFile());
            results.addProperty("documents", dbInfo.getDocCount());

            results.addProperty(InterfaceConst.TOPIC, targetProperties.get(InterfaceConst.TOPIC));
            results.addProperty(InterfaceConst.BATCH_SIZE, targetProperties.get(InterfaceConst
                    .BATCH_SIZE));
            results.addProperty(InterfaceConst.TASKS_MAX, targetProperties.get(InterfaceConst
                    .TASKS_MAX));
            results.add("testTimes", testTimes);
        } else {
            results.addProperty("testRounds", results.get("testRounds").getAsInt() + 1);
            results.get("testTimes").getAsJsonArray().add(testTime);
        }
        return results;
    }

    private long runTest() {
        // KAFKA
        HashMap<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();

        // 1. Create Connector and Trigger sourceTask to get a batch of records
        sinkTask.start(targetProperties);

        // 2. Measure SourceRecords
        long startTime = System.currentTimeMillis();

        sinkTask.put(sinkRecords);
        sinkTask.flush(offsets);

        long endTime = System.currentTimeMillis();

        //3. Stop sourceTask
        sinkTask.stop();

        return endTime - startTime;
    }

    @After
    public void tearDown() {
        CloudantDbUtils.dropDatabase(targetProperties);
    }

    @AfterClass
    public static void Results() {
        //Show Properties and BenchmarkRounds
        System.out.println("\n### Results - testSinkPerformance ###");
        ConnectorUtils.showPerformanceResults(testResults1);
        System.out.println("\n### Results - testSinkPerformance2 ###");
        ConnectorUtils.showPerformanceResults(testResults2);
        System.out.println("\n### Results - testSinkPerformance3 ###");
        ConnectorUtils.showPerformanceResults(testResults3);
    }
}
