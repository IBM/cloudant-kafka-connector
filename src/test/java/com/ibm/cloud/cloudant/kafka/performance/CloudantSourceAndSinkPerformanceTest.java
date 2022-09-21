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
import com.ibm.cloud.cloudant.kafka.common.InterfaceConst;
import com.ibm.cloud.cloudant.kafka.connect.CachedClientManager;
import com.ibm.cloud.cloudant.kafka.connect.CloudantSinkConnector;
import com.ibm.cloud.cloudant.kafka.connect.CloudantSinkTask;
import com.ibm.cloud.cloudant.kafka.connect.CloudantSourceConnector;
import com.ibm.cloud.cloudant.kafka.connect.CloudantSourceTask;
import com.ibm.cloud.cloudant.kafka.connect.utils.CloudantDbUtils;
import com.ibm.cloud.cloudant.kafka.connect.utils.ConnectorUtils;
import com.ibm.cloud.cloudant.v1.Cloudant;
import com.ibm.cloud.cloudant.v1.model.DatabaseInformation;
import org.apache.kafka.connect.connector.ConnectorContext;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.powermock.api.easymock.PowerMock;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class CloudantSourceAndSinkPerformanceTest extends AbstractBenchmark {
    private static Cloudant targetService;
    private static JsonObject testResults1 = new JsonObject();
    private static JsonObject testResults2 = new JsonObject();
    private static JsonObject testResults3 = new JsonObject();

    private Map<String, String> sourceProperties;
    private Map<String, String> targetProperties;

    private CloudantSinkTask sinkTask;
    private CloudantSourceTask sourceTask;

    private List<SourceRecord> sourceRecords;
    private List<SinkRecord> sinkRecords;

    private AtomicBoolean _runningSourceThread;
    private AtomicBoolean _runningSinkThread;

    @Before
    public void setUp() {
        //Set properties

        sourceProperties = ConnectorUtils.getTestProperties();
        sourceProperties.put(InterfaceConst.URL, sourceProperties.get(ConnectorUtils
                .PERFORMANCE_URL));
        targetProperties = ConnectorUtils.getTestProperties();
        targetProperties.put(InterfaceConst.URL, targetProperties.get(ConnectorUtils
                .PERFORMANCE_URL) + "_target");

        //Create SinkConnector
        CloudantSinkConnector sinkConnector = new CloudantSinkConnector();
        ConnectorContext context = PowerMock.createMock(ConnectorContext.class);
        sinkConnector.initialize(context);

        //Create SourceConnector
        CloudantSourceConnector sourceConnector = new CloudantSourceConnector();
        context = PowerMock.createMock(ConnectorContext.class);
        sourceConnector.initialize(context);

        // Create a _target database to replicate data into
        targetService = CachedClientManager.getInstance(targetProperties);

        //Create Sink Connector
        sourceTask = new CloudantSourceTask();
        sinkTask = new CloudantSinkTask();
    }

    @BenchmarkOptions(benchmarkRounds = 3, warmupRounds = 0)
    @Test
    public void testSourceAndSinkPerformance() {
        //set parameter => init(topic, batch.size, tasks.max, guid.schema)
        init("t01", 10000, 1, false);
        long testTime = runTest();
        testResults1 = addResults(testResults1, testTime);
    }

    @BenchmarkOptions(benchmarkRounds = 3, warmupRounds = 0)
    @Test
    public void testSourceAndSinkPerformance2() {
        //set parameter => init(topic, batch.size, tasks.max, guid.schema)
        init("t02", 10000, 1, false);
        long testTime = runTest();
        testResults2 = addResults(testResults2, testTime);
    }

    @BenchmarkOptions(benchmarkRounds = 3, warmupRounds = 0)
    @Test
    public void testSourceAndSinkPerformance3() {
        //set parameter => init(topic, batch.size, tasks.max, guid.schema)
        init("t03", 10000, 1, false);
        long testTime = runTest();
        testResults3 = addResults(testResults3, testTime);
    }

    private void init(String topics, int batch_size, int tasks_max, Boolean replication) {
        //set SourceProperties for SourceTasks
        sourceProperties.put(InterfaceConst.TOPIC, topics);
        sourceProperties.put(InterfaceConst.BATCH_SIZE, Integer.toString(batch_size));
        sourceProperties.put(InterfaceConst.TASKS_MAX, Integer.toString(tasks_max));

        //set TargetProperties for SinkTasks
        targetProperties.put(InterfaceConst.TOPIC, topics);
        targetProperties.put(InterfaceConst.BATCH_SIZE, Integer.toString(batch_size));
        targetProperties.put(InterfaceConst.TASKS_MAX, Integer.toString(tasks_max));
        targetProperties.put(InterfaceConst.REPLICATION, replication.toString());
    }

    private long runTest() {
        // 1. Set variables
        sinkRecords = new ArrayList<>();
        sourceRecords = new ArrayList<>();
        List<SinkRecord> tempRecords = Collections.synchronizedList(new ArrayList<SinkRecord>());

        _runningSourceThread = new AtomicBoolean(true);
        _runningSinkThread = new AtomicBoolean(true);

        // 2. Start source and sink task
        sourceTask.start(sourceProperties);
        sinkTask.start(targetProperties);

        // 3. Thread for source tasks
        Thread threadSourceTask = new Thread(() -> {
            try {
                do {
                    sourceRecords = sourceTask.poll();
                    for (SourceRecord record : sourceRecords) {
                        SinkRecord sinkRecord = new SinkRecord(
                                record.topic(), // topic
                                0, // partition
                                record.keySchema(), // key schema
                                record.key(), // key
                                record.valueSchema(), // value schema
                                record.value(),  // value
                                0); // offset
                        sinkRecords.add(sinkRecord);
                    }

                    synchronized (tempRecords) {
                        tempRecords.addAll(sinkRecords);
                    }
                    sinkRecords.clear();

                } while (sourceRecords.size() > 0);

                _runningSourceThread.set(false);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });

        // 4. Thread for sink tasks
        Thread threadSinkTask = new Thread(() -> {
            try {
                do {
                    if ((tempRecords.size() == 0)) {
                        Thread.sleep(100);
                    } else {
                        synchronized (tempRecords) {
                            sinkTask.put(tempRecords);
                            tempRecords.clear();
                        }
                    }
                }
                while (sourceRecords.size() > 0 || tempRecords.size() > 0 ||
                        CloudantDbUtils.getDbInfo(
                                targetProperties.get(InterfaceConst.DB),
                                targetService).getDocCount() == 0);

                _runningSinkThread.set(false);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });

        // 5. Start Threads and measure time
        long startTime = System.currentTimeMillis();

        threadSourceTask.start();
        threadSinkTask.start();

        // 5. Test Thread are running
        while (_runningSourceThread.get() || _runningSinkThread.get()) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                //LOG.error(e);
            }
        }

        long endTime = System.currentTimeMillis();

        // 6. Stop Threads
        threadSourceTask.interrupt();
        threadSinkTask.interrupt();

        // 7. Flush the latest set of records in case the batch has not been committed
        sinkTask.flush(null);
        sinkRecords.clear();

        // 8. Stop source and target task
        sourceTask.stop();
        sinkTask.stop();
        return endTime - startTime;
    }

    private JsonObject addResults(JsonObject results, long testTime) {
        if (results.size() == 0) {
            JsonArray testTimes = new JsonArray();
            testTimes.add(testTime);
            results.addProperty("testRounds", 1);
            DatabaseInformation dbInfo = CloudantDbUtils.getDbInfo(
                    targetProperties.get(InterfaceConst.DB) + "_target",
                    targetService);
            results.addProperty("diskSize", dbInfo.getSizes().getFile());
            results.addProperty("documents", dbInfo.getDocCount());

            //SourceProperties and TargetProperties should be equal
            results.addProperty(InterfaceConst.TOPIC, targetProperties.get(InterfaceConst.TOPIC));
            results.addProperty(InterfaceConst.BATCH_SIZE, targetProperties.get(InterfaceConst
                    .BATCH_SIZE));
            results.addProperty(InterfaceConst.TASKS_MAX, targetProperties.get(InterfaceConst
                    .TASKS_MAX));
            results.addProperty(InterfaceConst.REPLICATION, targetProperties.get(InterfaceConst
                    .REPLICATION));
            results.add("testTimes", testTimes);
        } else {
            results.addProperty("testRounds", results.get("testRounds").getAsInt() + 1);
            results.get("testTimes").getAsJsonArray().add(testTime);
        }
        return results;
    }

    @After
    public void tearDown() {
        CloudantDbUtils.dropDatabase(targetProperties);
    }

    @AfterClass
    public static void Results() {
        //Show Properties and BenchmarkRounds
        System.out.println("\n### Results - testSourceAndSinkPerformance ###");
        ConnectorUtils.showPerformanceResults(testResults1);
        System.out.println("\n### Results - testSourceAndSinkPerformance2 ###");
        ConnectorUtils.showPerformanceResults(testResults2);
        System.out.println("\n### Results - testSourceAndSinkPerformance3 ###");
        ConnectorUtils.showPerformanceResults(testResults3);
    }
}