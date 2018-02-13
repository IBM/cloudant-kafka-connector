/*
 * Copyright © 2017, 2018 IBM Corp. All rights reserved.
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
package com.ibm.cloudant.kafka.connect.utils;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.ibm.cloudant.kafka.common.InterfaceConst;
import com.ibm.cloudant.kafka.connect.CloudantSourceConnector;
import com.ibm.cloudant.kafka.connect.CloudantSourceTask;

import org.apache.kafka.connect.connector.ConnectorContext;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.easymock.EasyMock;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
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

    public static Map<String, String> getSourceProperties() throws IOException {
        Properties testProperties = readTestPropertiesFile();
        Map<String, String> sourceProperties = getCommonTestProperties(testProperties);

        sourceProperties.put(InterfaceConst.BATCH_SIZE, testProperties.getProperty(InterfaceConst
                .BATCH_SIZE) == null ? "500" : testProperties.getProperty(InterfaceConst
                .BATCH_SIZE));
        sourceProperties.put(InterfaceConst.LAST_CHANGE_SEQ, testProperties.getProperty
                (InterfaceConst.LAST_CHANGE_SEQ));

        return sourceProperties;
    }

    public static Map<String, String> getTargetProperties() throws IOException {
        Properties testProperties = readTestPropertiesFile();
        Map<String, String> targetProperties = getCommonTestProperties(testProperties);

        targetProperties.put(InterfaceConst.BATCH_SIZE, testProperties.getProperty(InterfaceConst
                .BATCH_SIZE));
        targetProperties.put(InterfaceConst.REPLICATION, testProperties.getProperty
                (InterfaceConst.REPLICATION));

        return targetProperties;
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
        if (testResults.has(InterfaceConst.REPLICATION)) {
            System.out.print("; " + InterfaceConst.REPLICATION + ": " + testResults.get
                    (InterfaceConst.REPLICATION));
        }

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

    public static Properties readTestPropertiesFile() throws IOException {
        Properties testProperties = new Properties();
        testProperties.load(new FileReader(new File("src/test/resources/test.properties")));
        return testProperties;
    }

    public static Map<String, String> getCommonTestProperties(Properties testProperties) {

        Map<String, String> connectorProperties = new HashMap<>();

        String systemPropAcct = System.getProperty("cloudant.account");
        // If a system property was used to specify a test account then use system property account
        // configuration ahead of the test.properties file and generate a random DB name
        if (systemPropAcct != null) {
            // URL, append a random database
            connectorProperties.put(InterfaceConst.URL, System.getProperty("cloudant.account") +
                    "/kafka-test-db-" + UUID.randomUUID().toString());
            // Username
            connectorProperties.put(InterfaceConst.USER_NAME, System.getProperty("cloudant.user"));
            // Password
            connectorProperties.put(InterfaceConst.PASSWORD, System.getProperty("cloudant.pass"));
        } else {
            // Fallback to the test.properties file
            connectorProperties.put(InterfaceConst.URL, testProperties.getProperty(InterfaceConst
                    .URL));
            connectorProperties.put(InterfaceConst.USER_NAME, testProperties.getProperty
                    (InterfaceConst.USER_NAME));
            connectorProperties.put(InterfaceConst.PASSWORD, testProperties.getProperty
                    (InterfaceConst.PASSWORD));
        }

        // Topic and tasks max are common to both configurations
        connectorProperties.put(InterfaceConst.TOPIC, testProperties.getProperty(InterfaceConst
                .TOPIC));
        connectorProperties.put(InterfaceConst.TASKS_MAX, testProperties.getProperty
                (InterfaceConst.TASKS_MAX));

        // Schema options
        connectorProperties.put(InterfaceConst.USE_VALUE_SCHEMA_STRUCT, testProperties
                .getProperty(InterfaceConst.USE_VALUE_SCHEMA_STRUCT));

        // Add the performance URL
        connectorProperties.put(PERFORMANCE_URL, testProperties.getProperty(PERFORMANCE_URL));

        return connectorProperties;
    }
}