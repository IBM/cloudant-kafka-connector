package com.ibm.cloudant.kafka.connect;

import java.io.File;
import java.io.FileReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.connect.connector.ConnectorContext;
import org.powermock.api.easymock.PowerMock;

import com.ibm.cloudant.kafka.common.InterfaceConst;

import junit.framework.Assert;
import junit.framework.TestCase;

public class CloudantSinkConnectorTest extends TestCase {

	private CloudantSinkConnector connector;
	private ConnectorContext context;
	private Map<String, String> targetProperties;
	
	Properties testProperties;

	protected void setUp() throws Exception {
		super.setUp();
		
		testProperties = new Properties();
		testProperties.load(new FileReader(new File("src/test/resources/test.properties")));
	
	    connector = new CloudantSinkConnector();
        context = PowerMock.createMock(ConnectorContext.class);
        connector.initialize(context);

        targetProperties = new HashMap<String, String>();
    	targetProperties.put(InterfaceConst.URL, testProperties.getProperty(InterfaceConst.URL));
		targetProperties.put(InterfaceConst.USER_NAME, testProperties.getProperty(InterfaceConst.USER_NAME));
		targetProperties.put(InterfaceConst.PASSWORD, testProperties.getProperty(InterfaceConst.PASSWORD));

        targetProperties.put(InterfaceConst.TOPIC, testProperties.getProperty(InterfaceConst.TOPIC));
	}


	public void testTaskConfigsInt() {
	    PowerMock.replayAll();
        connector.start(targetProperties);
        PowerMock.verifyAll();
	}

	public void testConfig() {
	    PowerMock.replayAll();
        connector.start(targetProperties);
        
        List<Map<String, String>> taskConfigs = connector.taskConfigs(1);

        Assert.assertEquals(1, taskConfigs.size());
 
        Assert.assertEquals(testProperties.getProperty(InterfaceConst.TOPIC), 
        		taskConfigs.get(0).get(InterfaceConst.TOPIC));
        PowerMock.verifyAll();
	}

}
