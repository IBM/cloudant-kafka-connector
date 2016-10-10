/**
 * 
 */
package com.ibm.cloudant.kafka.connect;

import java.util.List;
import java.io.File;
import java.io.FileReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.connect.connector.ConnectorContext;
import org.powermock.api.easymock.PowerMock;

import com.ibm.cloudant.kafka.common.InterfaceConst;

import junit.framework.Assert;
import junit.framework.TestCase;

/**
 * @author holger
 *
 */
public class CloudantSourceConnectorTest extends TestCase {

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
	public void testTaskConfigsInt() {
	    PowerMock.replayAll();
        connector.start(sourceProperties);
        
        List<Map<String, String>> taskConfigs = connector.taskConfigs(1);

        Assert.assertEquals(1, taskConfigs.size());
        Assert.assertEquals(testProperties.getProperty(InterfaceConst.URL), taskConfigs.get(0).get(InterfaceConst.URL));
        Assert.assertEquals(testProperties.getProperty(InterfaceConst.USER_NAME), taskConfigs.get(0).get(InterfaceConst.USER_NAME));
        Assert.assertEquals(testProperties.getProperty(InterfaceConst.PASSWORD), taskConfigs.get(0).get(InterfaceConst.PASSWORD));

        Assert.assertEquals(testProperties.getProperty(InterfaceConst.TOPIC), taskConfigs.get(0).get(InterfaceConst.TOPIC));
        PowerMock.verifyAll();
	}

}
