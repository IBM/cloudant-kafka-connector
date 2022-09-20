/*
 * Copyright © 2022 IBM Corp. All rights reserved.
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

import com.ibm.cloud.cloudant.kafka.common.CloudantConst;
import com.ibm.cloud.cloudant.kafka.connect.utils.ServiceCallUtils;
import com.ibm.cloud.cloudant.v1.Cloudant;
import com.ibm.cloud.cloudant.v1.model.Change;
import com.ibm.cloud.cloudant.v1.model.ChangesResult;
import com.ibm.cloud.cloudant.v1.model.ChangesResultItem;
import com.ibm.cloud.cloudant.v1.model.Document;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Assert;
import org.junit.Test;
import org.powermock.api.easymock.PowerMock;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;

public class TombstoneTest {

    private static final String connectionName = "_mock";

    @Test
    public void testDeletedDocumentEmitsTombstones() throws Exception{

        //
        // given
        //

        // chain of mocks is Cloudant (client) response -> changes result -> changes result item -> change -> document
        Cloudant mockCloudant = PowerMock.createMock(Cloudant.class);
        ChangesResult mockChangesResult = PowerMock.createMock(ChangesResult.class);
        ChangesResultItem mockChangesResultItem = PowerMock.createMock(ChangesResultItem.class);
        Change mockChange = PowerMock.createMock(Change.class);
        // NB document not mocked
        String id = "123";
        Document document = new Document();
        document.setId(id);
        document.setDeleted(Boolean.TRUE);
        // expects in same order as above (could mock ServiceCall, but it's just as easy to use an anonymous class)
        expect(mockCloudant.postChanges(anyObject())).andReturn(ServiceCallUtils.makeServiceCallWithResult(mockChangesResult)).anyTimes();
        // NB mocked twice - first for logging call, second for actual use
        expect(mockChangesResult.getResults()).andReturn(Collections.singletonList(mockChangesResultItem)).times(2);
        expect(mockChangesResult.getLastSeq()).andReturn("100");
        expect(mockChangesResultItem.getChanges()).andReturn(Collections.singletonList(mockChange));
        expect(mockChangesResultItem.getSeq()).andReturn("123-abc");
        expect(mockChangesResultItem.getId()).andReturn(id);
        expect(mockChangesResultItem.getDoc()).andReturn(document);
        expect(mockChangesResultItem.isDeleted()).andReturn(Boolean.TRUE);
        
        // force the task to use our mock client
        ClientManagerUtils.addClientToCache(connectionName, mockCloudant);
        // setup task with some minimal config
        CloudantSourceTask cloudantSourceTask = new CloudantSourceTask();
        Map<String, String> configMap = new HashMap<>();
        configMap.put("name", connectionName);
        configMap.put("cloudant.since", "1000");
        configMap.put("cloudant.url", "http://foo");
        configMap.put("cloudant.db", "foo");
        configMap.put("topics", "foo");

        //
        // when
        //

        replay(mockCloudant);
        replay(mockChangesResult);
        replay(mockChange);
        replay(mockChangesResultItem);
        cloudantSourceTask.start(configMap);
        List<SourceRecord> records = cloudantSourceTask.poll();

        //
        // then
        //
        Assert.assertEquals(2, records.size());
        Assert.assertNotNull(records.get(0).value());
        Assert.assertNull(records.get(1).value());
        Assert.assertEquals(Collections.singletonMap(CloudantConst.CLOUDANT_DOC_ID, id), records.get(0).key());
        Assert.assertEquals(Collections.singletonMap(CloudantConst.CLOUDANT_DOC_ID, id), records.get(1).key());
    }

}
