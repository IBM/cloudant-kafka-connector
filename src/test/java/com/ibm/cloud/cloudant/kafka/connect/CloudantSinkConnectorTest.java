/*
 * Copyright © 2016, 2022 IBM Corp. All rights reserved.
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
package com.ibm.cloud.cloudant.kafka.connect;


import com.ibm.cloud.cloudant.kafka.common.InterfaceConst;
import com.ibm.cloud.cloudant.kafka.connect.utils.ConnectorUtils;
import junit.framework.TestCase;
import org.apache.kafka.connect.connector.ConnectorContext;
import org.junit.Assert;
import org.powermock.api.easymock.PowerMock;

import java.util.List;
import java.util.Map;

/**
 * @author holger
 */
public class CloudantSinkConnectorTest extends TestCase {

    private CloudantSinkConnector connector;
    private Map<String, String> targetProperties;

    protected void setUp() throws Exception {
        super.setUp();

        targetProperties = ConnectorUtils.getTestProperties();

        connector = new CloudantSinkConnector();
        ConnectorContext context = PowerMock.createMock(ConnectorContext.class);
        connector.initialize(context);
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

        Assert.assertEquals(targetProperties.get(InterfaceConst.URL), taskConfigs.get(0).get
                (InterfaceConst.URL));
        Assert.assertEquals(targetProperties.get(InterfaceConst.USERNAME), taskConfigs.get(0)
                .get(InterfaceConst.USERNAME));
        Assert.assertEquals(targetProperties.get(InterfaceConst.PASSWORD), taskConfigs.get(0).get
                (InterfaceConst.PASSWORD));
        Assert.assertEquals(targetProperties.get(InterfaceConst.TOPIC), taskConfigs.get(0).get
                (InterfaceConst.TOPIC));

        Assert.assertEquals(targetProperties.get(InterfaceConst.TASKS_MAX), taskConfigs.get(0)
                .get(InterfaceConst.TASKS_MAX));
        Assert.assertEquals(targetProperties.get(InterfaceConst.BATCH_SIZE), taskConfigs.get(0)
                .get(InterfaceConst.BATCH_SIZE));

        PowerMock.verifyAll();
    }
}
