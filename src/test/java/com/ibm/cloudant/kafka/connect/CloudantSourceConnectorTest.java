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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.connect.connector.ConnectorContext;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.rules.ExpectedException;
import org.powermock.api.easymock.PowerMock;

import com.ibm.cloudant.kafka.common.InterfaceConst;

import junit.framework.TestCase;

/**
 * @author holger
 *
 */
public class CloudantSourceConnectorTest extends TestCase {

	@Rule public ExpectedException thrown= ExpectedException.none();
	
	private CloudantSourceConnector connector;
	private ConnectorContext context;
	private Map<String, String> sourceProperties;
	
	Properties testProperties;
	
	
	/* (non-Javadoc)
	 * @see junit.framework.TestCase#setUp()
	 */
	protected void setUp() throws Exception {
		super.setUp();
		
		testProperties = new Properties();
		testProperties.load(new FileReader(new File("src/test/resources/test.properties")));
		
	    connector = new CloudantSourceConnector();
        context = PowerMock.createMock(ConnectorContext.class);
        connector.initialize(context);

        sourceProperties = new HashMap<String, String>();
        
        sourceProperties.put(InterfaceConst.URL,testProperties.getProperty(InterfaceConst.URL));
        sourceProperties.put(InterfaceConst.USER_NAME, testProperties.getProperty(InterfaceConst.USER_NAME));
        sourceProperties.put(InterfaceConst.PASSWORD, testProperties.getProperty(InterfaceConst.PASSWORD));

        sourceProperties.put(InterfaceConst.TOPIC, testProperties.getProperty(InterfaceConst.TOPIC));
        sourceProperties.put(InterfaceConst.TASKS_MAX, testProperties.getProperty(InterfaceConst.TASKS_MAX));
        sourceProperties.put(InterfaceConst.BATCH_SIZE, testProperties.getProperty(InterfaceConst.BATCH_SIZE));
        
        sourceProperties.put(InterfaceConst.LAST_CHANGE_SEQ, testProperties.getProperty(InterfaceConst.LAST_CHANGE_SEQ));

	}

	/**
	 * Test method for {@link com.ibm.cloudant.kafka.connect.CloudantSourceConnector#stop()}.
	 */
	public void testStop() {
		// fail("Not yet implemented");
	}


	/**
	 * Test method for {@link com.ibm.cloudant.kafka.connect.CloudantSourceConnector#start(java.util.Map)}.
	 */
	public void testStartMapOfStringString() {
	    PowerMock.replayAll();
        connector.start(sourceProperties);
        PowerMock.verifyAll();
	}

	/**
	 * Test method for {@link com.ibm.cloudant.kafka.connect.CloudantSourceConnector#taskConfigs(int)}.
	 */
	public void testTaskConfigs() {
	    PowerMock.replayAll();
        connector.start(sourceProperties);
        
        int tasks_max = new Integer(sourceProperties.get(InterfaceConst.TASKS_MAX)).intValue();
        List<Map<String, String>> taskConfigs = connector.taskConfigs(tasks_max);

        Assert.assertEquals(1, taskConfigs.size());
        
        Assert.assertEquals(testProperties.getProperty(InterfaceConst.URL), taskConfigs.get(0).get(InterfaceConst.URL));
        Assert.assertEquals(testProperties.getProperty(InterfaceConst.USER_NAME), taskConfigs.get(0).get(InterfaceConst.USER_NAME));
        Assert.assertEquals(testProperties.getProperty(InterfaceConst.PASSWORD), taskConfigs.get(0).get(InterfaceConst.PASSWORD));
        Assert.assertEquals(testProperties.getProperty(InterfaceConst.LAST_CHANGE_SEQ), taskConfigs.get(0).get(InterfaceConst.LAST_CHANGE_SEQ));
        
        Assert.assertEquals(testProperties.getProperty(InterfaceConst.TOPIC), taskConfigs.get(0).get(InterfaceConst.TOPIC));
        
        Assert.assertEquals(testProperties.getProperty(InterfaceConst.TASKS_MAX), taskConfigs.get(0).get(InterfaceConst.TASKS_MAX));
        Assert.assertEquals(testProperties.getProperty(InterfaceConst.BATCH_SIZE), taskConfigs.get(0).get(InterfaceConst.BATCH_SIZE));
        
        PowerMock.verifyAll();
	}
}
