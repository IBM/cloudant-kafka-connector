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

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.json.JSONArray;

import com.cloudant.client.api.Database;
import com.cloudant.client.api.model.ChangesResult;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.ibm.cloudant.kafka.common.InterfaceConst;
import com.ibm.cloudant.kafka.common.utils.JavaCloudantUtil;
import com.ibm.cloudant.kakfa.connect.utils.CloudantDbUtils;

import junit.framework.TestCase;

/**
 * @author holger
 *
 */
public class CloudantSinkTaskTest extends TestCase {

	private CloudantSinkTask task;
    private ByteArrayOutputStream os;
	private HashMap<String, String> targetProperties;
	
	JSONArray data = null;
	
	Properties testProperties;
	
	/* (non-Javadoc)
	 * @see junit.framework.TestCase#setUp()
	 */
	protected void setUp() throws Exception {
		super.setUp();
		
		testProperties = new Properties();
		testProperties.load(new FileReader(new File("src/test/resources/test.properties")));
		
		task = new CloudantSinkTask();
		os = new ByteArrayOutputStream();
	      
		targetProperties = new HashMap<String, String>();
		
		targetProperties.put(InterfaceConst.URL, testProperties.getProperty(InterfaceConst.URL));
		targetProperties.put(InterfaceConst.USER_NAME, testProperties.getProperty(InterfaceConst.USER_NAME));
		targetProperties.put(InterfaceConst.PASSWORD, testProperties.getProperty(InterfaceConst.PASSWORD));
	
		targetProperties.put(InterfaceConst.TASKS_MAX, testProperties.getProperty(InterfaceConst.TASKS_MAX));
		targetProperties.put(InterfaceConst.BATCH_SIZE, testProperties.getProperty(InterfaceConst.BATCH_SIZE));
    	  
		targetProperties.put(InterfaceConst.TOPIC, testProperties.getProperty(InterfaceConst.TOPIC));
		
		targetProperties.put(InterfaceConst.GUID_SCHEMA, testProperties.getProperty(InterfaceConst.GUID_SCHEMA));		
	}

	/**
	 * Test method for {@link com.ibm.cloudant.kafka.connect.CloudantSinkTask#put(java.util.Collection)}.
	 */
	public void testPutCollectionOfSinkRecord() {
		
		// CLOUDANT
		Database db = JavaCloudantUtil.getDBInst(
				targetProperties.get(InterfaceConst.URL), 
				targetProperties.get(InterfaceConst.USER_NAME),
				targetProperties.get(InterfaceConst.PASSWORD));
		
		// Get the current update sequence
		String since = db.info().getUpdateSeq(); // latest update seq
		
		// KAFKA
		HashMap<TopicPartition, OffsetAndMetadata> offsets = new HashMap<TopicPartition, OffsetAndMetadata>();
		  
		// Emit 3 new documents
		task.start(targetProperties);
		
		JsonParser parser = new JsonParser();

		JsonObject doc1 = parser.parse("{\"_id\":\"doc1\","
				+ "\"key\":\"value1\"}").getAsJsonObject();
		
		task.put(Arrays.asList(
				new SinkRecord(testProperties.getProperty(InterfaceConst.TOPIC), 0, 
						null, null, Schema.STRING_SCHEMA, doc1, 1)));
		
	/*	offsets.put(new TopicPartition(testProperties.getProperty(InterfaceConst.TOPIC), 0), 
				new OffsetAndMetadata(1L));
		*/
		task.flush(offsets);

		JsonObject doc2 = parser.parse("{\"_id\":\"doc2\","
				+ "\"key\":\"value2\"}").getAsJsonObject();
		
		JsonObject doc3 = parser.parse("{\"_id\":\"doc3\","
				+ "\"key\":\"value3\"}").getAsJsonObject();
		
		task.put(Arrays.asList(
				new SinkRecord(testProperties.getProperty(InterfaceConst.TOPIC), 
						0, null, null, Schema.STRING_SCHEMA,doc2.toString(), 2),
				
				new SinkRecord(testProperties.getProperty(InterfaceConst.TOPIC), 
						0, null, null, Schema.STRING_SCHEMA, doc3.toString(), 3)
				));
	/*	
		offsets.put(new TopicPartition(testProperties.getProperty(InterfaceConst.TOPIC), 0), 
				new OffsetAndMetadata(2L));
		offsets.put(new TopicPartition(testProperties.getProperty(InterfaceConst.TOPIC), 0),
				new OffsetAndMetadata(3L));
				*/
		task.flush(offsets);
		
		// CLOUDANT
		ChangesResult changeResult = db.changes()
				.since(since)
				.limit(4)
				.includeDocs(true)
				.getChanges();

		 //process the ChangesResult
		List<JsonElement> result = new ArrayList<JsonElement>();
		
		for (ChangesResult.Row row : changeResult.getResults()) {
			 JsonElement key = row.getDoc().get("key");
			 result.add(key);
		}
		 
		assertEquals(3, result.size());
	
		assertTrue(result.contains(doc1.get("key")));
		assertTrue(result.contains(doc2.get("key")));
		assertTrue(result.contains(doc3.get("key")));
	}

	/**
	 * @return the targetProperties
	 */
	public HashMap<String, String> getTargetProperties() {
		return targetProperties;
	}

	/**
	 * @param targetProperties the targetProperties to set
	 */
	public void setTargetProperties(HashMap<String, String> targetProperties) {
		this.targetProperties = targetProperties;
	}

	/**
	 * @return the task
	 */
	public CloudantSinkTask getTask() {
		return task;
	}

	/**
	 * @param task the task to set
	 */
	public void setTask(CloudantSinkTask task) {
		this.task = task;
	}
	
	protected void tearDown() throws Exception {

		// Remove the created database
		CloudantDbUtils.dropDatabase(targetProperties.get(InterfaceConst.URL), 
				targetProperties.get(InterfaceConst.USER_NAME), 
				targetProperties.get(InterfaceConst.PASSWORD));
	}
}
