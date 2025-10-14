/*
 * Copyright © 2016, 2023 IBM Corp. All rights reserved.
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
package com.ibm.cloud.cloudant.kafka;

import com.ibm.cloud.cloudant.kafka.caching.CachedClientManager;
import com.ibm.cloud.cloudant.kafka.caching.ClientManagerUtils;
import com.ibm.cloud.cloudant.kafka.utils.InterfaceConst;
import com.ibm.cloud.cloudant.kafka.utils.ConnectorUtils;
import com.ibm.cloud.cloudant.v1.Cloudant;
import junit.framework.TestCase;
import org.apache.kafka.connect.connector.ConnectorContext;
import org.junit.Assert;
import org.powermock.api.easymock.PowerMock;
import org.powermock.reflect.Whitebox;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author holger
 */
public class SourceChangesConnectorTest extends TestCase {

    private SourceChangesConnector connector;
    private Map<String, String> sourceProperties;


    /* (non-Javadoc)
     * @see junit.framework.TestCase#setUp()
     */
    protected void setUp() throws Exception {
        super.setUp();

        sourceProperties = ConnectorUtils.getTestProperties();

        connector = new SourceChangesConnector();
        ConnectorContext context = PowerMock.createMock(ConnectorContext.class);
        connector.initialize(context);
    }

    /**
     * Test method for {@link SourceChangesConnector#stop()}.
     */
    public void testStop() {
        Map<String, Cloudant> clientCache = new ConcurrentHashMap<>();
        Whitebox.setInternalState(CachedClientManager.class, "clientCache", clientCache);
        ClientManagerUtils.addClientToCache(sourceProperties.get("name"), PowerMock.createMock(Cloudant.class));

        Assert.assertFalse(clientCache.isEmpty());
        connector.start(sourceProperties);
        Assert.assertFalse(clientCache.isEmpty());
        connector.stop();
        Assert.assertTrue(clientCache.isEmpty());
    }


    /**
     * Test method for
     * {@link SourceChangesConnector#start(java.util.Map)}.
     */
    public void testStartMapOfStringString() {
        PowerMock.replayAll();
        connector.start(sourceProperties);
        PowerMock.verifyAll();
    }

    /**
     * Test method for
     * {@link SourceChangesConnector#taskConfigs(int)}.
     */
    public void testTaskConfigs() {
        PowerMock.replayAll();
        connector.start(sourceProperties);

        int tasks_max = Integer.parseInt(sourceProperties.get(InterfaceConst.TASKS_MAX));
        List<Map<String, String>> taskConfigs = connector.taskConfigs(tasks_max);

        Assert.assertEquals(1, taskConfigs.size());

        Assert.assertEquals(sourceProperties.get(InterfaceConst.URL), taskConfigs.get(0).get
                (InterfaceConst.URL));
        Assert.assertEquals(sourceProperties.get(InterfaceConst.USERNAME), taskConfigs.get(0)
                .get(InterfaceConst.USERNAME));
        Assert.assertEquals(sourceProperties.get(InterfaceConst.PASSWORD), taskConfigs.get(0).get
                (InterfaceConst.PASSWORD));
        Assert.assertEquals(sourceProperties.get(InterfaceConst.LAST_CHANGE_SEQ), taskConfigs.get
                (0).get(InterfaceConst.LAST_CHANGE_SEQ));

        Assert.assertEquals(sourceProperties.get(InterfaceConst.TOPIC), taskConfigs.get(0).get
                (InterfaceConst.TOPIC));

        Assert.assertEquals(sourceProperties.get(InterfaceConst.TASKS_MAX), taskConfigs.get(0)
                .get(InterfaceConst.TASKS_MAX));
        Assert.assertEquals(sourceProperties.get(InterfaceConst.BATCH_SIZE), taskConfigs.get(0)
                .get(InterfaceConst.BATCH_SIZE));

        PowerMock.verifyAll();
    }
}
