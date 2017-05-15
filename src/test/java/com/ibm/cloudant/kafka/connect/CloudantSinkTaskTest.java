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
	private HashMap<String, String> targetProperties;
	
	JSONArray data = null;
	
	JsonParser parser;
	JsonObject doc1, doc2, doc3;
	
	Properties testProperties;
	
	/* (non-Javadoc)
	 * @see junit.framework.TestCase#setUp()
	 */
	protected void setUp() throws Exception {
		super.setUp();
		
		testProperties = new Properties();
		testProperties.load(new FileReader(new File("src/test/resources/test.properties")));
		
		task = new CloudantSinkTask();
	      
		targetProperties = new HashMap<String, String>();
		
		targetProperties.put(InterfaceConst.URL, testProperties.getProperty(InterfaceConst.URL));
		targetProperties.put(InterfaceConst.USER_NAME, testProperties.getProperty(InterfaceConst.USER_NAME));
		targetProperties.put(InterfaceConst.PASSWORD, testProperties.getProperty(InterfaceConst.PASSWORD));
	
		targetProperties.put(InterfaceConst.TASKS_MAX, testProperties.getProperty(InterfaceConst.TASKS_MAX));
		targetProperties.put(InterfaceConst.BATCH_SIZE, testProperties.getProperty(InterfaceConst.BATCH_SIZE));
    	  
		targetProperties.put(InterfaceConst.TOPIC, testProperties.getProperty(InterfaceConst.TOPIC));
		
		//Test objects
		parser = new JsonParser();
		doc1 = parser.parse("{\"_id\":\"doc1\","
				+ "\"number\":1,"
				+ "\"key\":\"value1\"}").getAsJsonObject();
		
		doc2 = parser.parse("{\"_id\":\"doc2\","
				+ "\"number\":2,"
				+ "\"key\":\"value2\"}").getAsJsonObject();
		
		doc3 = parser.parse("{\"_id\":\"doc3\","
				+ "\"number\":3,"
				+ "\"key\":\"value3\"}").getAsJsonObject();
	}

	/**
	 * Test method for {@link com.ibm.cloudant.kafka.connect.CloudantSinkTask#put(java.util.Collection)}.
	 */
	public void testReplicateSinkRecordSchema() {
		targetProperties.put(InterfaceConst.REPLICATION, "true");				
		List<JsonObject> result = testPutCollectionOfSinkRecord();				
				
		//Test results
		assertEquals(3, result.size());	
		assertTrue(result.contains(doc1));
		assertTrue(result.contains(doc2));
		assertTrue(result.contains(doc3));
	}
	
	/**
	 * Test method for {@link com.ibm.cloudant.kafka.connect.CloudantSinkTask#put(java.util.Collection)}.
	 */
	public void testNonReplicateSinkRecordSchema() {
		targetProperties.put(InterfaceConst.REPLICATION, "false");		
		List<JsonObject> result = testPutCollectionOfSinkRecord();	
		
		//Add Information (id_schema, kcschema)
		JsonObject kcschema = new JsonObject();
		kcschema.addProperty("type", "STRING");
		kcschema.addProperty("optional", false);		
		
		doc1.add("kcschema", kcschema);
		doc1.addProperty("_id", 
				targetProperties.get(InterfaceConst.TOPIC) + 
				"_" + 0 + "_" + doc1.get("number") + 
				"_" + doc1.get("_id").getAsString());	
		
		doc2.add("kcschema", kcschema);
		doc2.addProperty("_id", 
				targetProperties.get(InterfaceConst.TOPIC) + 
				"_" + 0 + "_" + doc2.get("number") + 
				"_" + doc2.get("_id").getAsString());
		
		doc3.add("kcschema", kcschema);
		doc3.addProperty("_id", 
				targetProperties.get(InterfaceConst.TOPIC) + 
				"_" + 0 + "_" + doc3.get("number") + 
				"_" + doc3.get("_id").getAsString());
										
		//Test results		
		assertEquals(3, result.size());	
		assertTrue(result.contains(doc1));
		assertTrue(result.contains(doc2));
		assertTrue(result.contains(doc3));
	}
		
	public List<JsonObject> testPutCollectionOfSinkRecord() {	
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
		
		task.put(Arrays.asList(
				new SinkRecord(testProperties.getProperty(InterfaceConst.TOPIC), 0, 
						null, null, Schema.STRING_SCHEMA, doc1, doc1.get("number").getAsLong())));
		
	/*	offsets.put(new TopicPartition(testProperties.getProperty(InterfaceConst.TOPIC), 0), 
				new OffsetAndMetadata(1L));
		*/
		task.flush(offsets);
		
		task.put(Arrays.asList(
				new SinkRecord(testProperties.getProperty(InterfaceConst.TOPIC), 
						0, null, null, Schema.STRING_SCHEMA,doc2.toString(), doc2.get("number").getAsLong()),
				
				new SinkRecord(testProperties.getProperty(InterfaceConst.TOPIC), 
						0, null, null, Schema.STRING_SCHEMA, doc3.toString(), doc3.get("number").getAsLong())
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
		List<JsonObject> result = new ArrayList<JsonObject>();
		for (ChangesResult.Row row : changeResult.getResults()) {
			 JsonObject doc = row.getDoc();
			 doc.remove("_rev");
			 result.add(doc);
		}
		
		return result;
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
