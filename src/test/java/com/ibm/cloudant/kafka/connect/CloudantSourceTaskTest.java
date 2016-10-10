package com.ibm.cloudant.kafka.connect;

import java.io.File;
import java.io.FileReader;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.connect.connector.ConnectorContext;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.easymock.EasyMock;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.powermock.api.easymock.PowerMock;

import com.ibm.cloudant.kafka.common.CloudantConst;
import com.ibm.cloudant.kafka.common.InterfaceConst;

import junit.framework.TestCase;

public class CloudantSourceTaskTest extends TestCase {
	
	private static int TEST_EXECUTION_IN_SEC = 10;
	
	private CloudantSourceConnector connector;
	private CloudantSourceTask task;
	private ConnectorContext context;
	SourceTaskContext taskContext;
	private Map<String, String> sourceProperties;
	OffsetStorageReader offsetReader;
	
	Properties testProperties;
	
	protected void setUp() throws Exception {
		super.setUp();

		testProperties = new Properties();
		testProperties.load(new FileReader(new File("src/test/resources/test.properties")));
		
		connector = new CloudantSourceConnector();
		context = EasyMock.mock(ConnectorContext.class);
		
		connector.initialize(context);

		sourceProperties = new HashMap<String, String>();
		sourceProperties.put(InterfaceConst.URL,testProperties.getProperty(InterfaceConst.URL));
		sourceProperties.put(InterfaceConst.USER_NAME, testProperties.getProperty(InterfaceConst.USER_NAME));
		sourceProperties.put(InterfaceConst.PASSWORD, testProperties.getProperty(InterfaceConst.PASSWORD));

		sourceProperties.put(InterfaceConst.TOPIC, testProperties.getProperty(InterfaceConst.TOPIC));

		connector.start(sourceProperties);
		
		taskContext = EasyMock.mock(SourceTaskContext.class);
	
		task = new CloudantSourceTask();
		offsetReader = PowerMock.createMock(OffsetStorageReader.class);
		
		task.initialize(taskContext);
	}

	private void expectOffsetLookupReturnNone() {
	     //   EasyMock.expect(taskContext.offsetStorageReader()).andReturn(offsetReader);
	     //   EasyMock.expect(offsetReader.offset(EasyMock.anyObject(Map.class))).andReturn(null);
	    }
	  
	@SuppressWarnings("unchecked")
	public void testStartMapOfStringString() throws InterruptedException {
		expectOffsetLookupReturnNone();
		PowerMock.replayAll();
	        
		// Run the task and process all documents currently in the _changes feed
		// EasyMock.replay(taskContext);
		// EasyMock.replay(task);
		task.start(sourceProperties);
		List<SourceRecord> records = task.poll();
		assertTrue(records.size() > 0);
		
		// Inspect the first record and make sure it is a valid Cloudant doc
		SourceRecord firstRecord = records.get(0);
		String firstValue = firstRecord.value().toString();
		
		JSONTokener tokener = new JSONTokener(firstValue);
		
		JSONObject firstObject = new JSONObject(tokener);
		assertNotNull(firstObject);
		
		assertNotNull(firstObject.get(CloudantConst.CLOUDANT_DOC_ID));
		assertNotNull(firstObject.get(CloudantConst.CLOUDANT_REV));
		
	}
	
	public void testStartWithOffset() throws InterruptedException {
		expectOffsetLookupReturnNone();
		PowerMock.replayAll();

		// Test again with a last sequence number > 0
		Collections.singletonMap(InterfaceConst.URL, "281-g1AAAAGjeJzLYWBgYMlgTmGQT0lKzi9KdUhJMtZLykxPyilN1UvOyS9NScwr0ctLLckBKmRKZEiy____f1YGc6JLLlCAPSU5xcLYIpmwdlQrTHBbkeQAJJPqoba4g21JMkpMMjFJJGwC0R7JYwGSDA1ACmjRfpBNfmCbLMwtElPNTanoH4hNByA2gf3kALbJNCXZKDnRnLApWQBd24kW");

		task.start(sourceProperties);
		List<SourceRecord> records2 = task.poll();
			
		// Second result should contain exactly one record less than first result
		// assertTrue(records2.size() == 4);
		// String latestSequenceNumber = (String) offset.get(InterfaceConst.LAST_CHANGE_SEQ);

	}
}
