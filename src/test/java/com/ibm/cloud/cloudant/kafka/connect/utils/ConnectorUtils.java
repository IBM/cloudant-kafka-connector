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
package com.ibm.cloud.cloudant.kafka.connect.utils;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.ibm.cloud.cloudant.kafka.common.InterfaceConst;
import com.ibm.cloud.cloudant.kafka.connect.CloudantConnectorConfig;
import com.ibm.cloud.cloudant.kafka.connect.CloudantSourceConnector;
import com.ibm.cloud.cloudant.kafka.connect.CloudantSourceTask;
import org.apache.kafka.connect.connector.ConnectorContext;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.easymock.EasyMock;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class ConnectorUtils {

    public static final String PERFORMANCE_URL = "performance.url";

    public static CloudantSourceTask createCloudantSourceConnector(Map<String, String> properties) {
        CloudantSourceConnector connector = new CloudantSourceConnector();
        CloudantSourceTask task = new CloudantSourceTask();

        ConnectorContext context = EasyMock.mock(ConnectorContext.class);
        SourceTaskContext taskContext = EasyMock.mock(SourceTaskContext.class);

        connector.initialize(context);
        connector.start(properties);

        task.initialize(taskContext);
        return task;
    }

    public static void showPerformanceResults(JsonObject testResults) {
        float diskSizeInMB = testResults.get("diskSize").getAsLong() / (1024 * 1024);

        //Show TestProperties
        System.out.println("Rounds: " + testResults.get("testRounds") + "; Documents: " +
                testResults.get("documents") + "; DB Size: " + diskSizeInMB + " MB");
        System.out.print(InterfaceConst.TOPIC + ": " + testResults.get(InterfaceConst.TOPIC));
        System.out.print("; " + InterfaceConst.BATCH_SIZE + ": " + testResults.get(InterfaceConst
                .BATCH_SIZE));
        System.out.print("; " + InterfaceConst.TASKS_MAX + ": " + testResults.get(InterfaceConst
                .TASKS_MAX));

        //Show TestResults
        System.out.print("\nTime: " + average(getTestTimes(testResults)) / 1000f + " Seconds");
        System.out.println("[+- " + stdDeviation(getTestTimes(testResults)) / 1000f + "]");
        System.out.println("Documents per second: " + testResults.get("documents").getAsLong() /
                (average(getTestTimes(testResults)) / 1000f));
        System.out.println("Data per second: " + diskSizeInMB / (average(getTestTimes
                (testResults)) / 1000f) + " MB/s");
    }

    public static ArrayList<Long> getTestTimes(JsonObject testResults) {
        ArrayList<Long> list = new ArrayList<Long>();
        JsonArray testTimes = testResults.get("testTimes").getAsJsonArray();
        if (testTimes != null) {
            for (int i = 0; i < testTimes.size(); i++) {
                list.add(testTimes.get(i).getAsLong());
            }
        }
        return list;
    }

    public static double average(ArrayList<Long> list) {
        if (list == null || list.isEmpty()) {
            return 0.0;
        }

        long sum = 0;
        for (int i = 0; i < list.size(); i++) {
            sum += list.get(i);
        }

        return ((double) sum) / list.size();
    }

    public static double stdDeviation(ArrayList<Long> list) {
        double sumDiffsSquared = 0.0;
        double avg = average(list);
        for (long value : list) {
            double diff = value - avg;
            diff *= diff;
            sumDiffsSquared += diff;
        }
        return Math.sqrt(sumDiffsSquared / (list.size() - 1));
    }

    public static Map<String, String> getTestProperties() {

        Map<String, String> connectorProperties = new HashMap<>();

        // some hardcoded properties we don't normally need to override
        connectorProperties.put(InterfaceConst.DB, "kafka-test-db-" + UUID.randomUUID());
        connectorProperties.put(InterfaceConst.TOPIC, "test-topic");
        connectorProperties.put(InterfaceConst.TASKS_MAX, "1");
        connectorProperties.put(InterfaceConst.BATCH_SIZE, "10000");
        // cached connection manager needs this - in real life this would be the name set in the connector properties file
        connectorProperties.put("name", "cloudant-connector-test");

        Collection<String> properties = new ArrayList<>();
        properties.addAll(CloudantConnectorConfig.CONFIG_DEF.configKeys().keySet());
        properties.addAll(connectorProperties.keySet());

        // use provided values from System.getProperties (can also override defaults above)
        for (String k : properties) {
            String v = System.getProperty(k);
            if (v != null) {
                connectorProperties.put(k, v);
            }
        }

        return connectorProperties;
    }
}
