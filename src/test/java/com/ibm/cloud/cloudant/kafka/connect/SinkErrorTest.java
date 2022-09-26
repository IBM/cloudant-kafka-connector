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

import com.ibm.cloud.cloudant.kafka.connect.utils.ServiceCallUtils;
import com.ibm.cloud.cloudant.v1.Cloudant;
import com.ibm.cloud.cloudant.v1.model.DocumentResult;
import com.ibm.cloud.cloudant.v1.model.Ok;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.powermock.api.easymock.PowerMock;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;

public class SinkErrorTest {

    private static final String connectionName = "_mock";

    // test that record reporter is invoked for one bad document, but not for the other good document
    @Test
    public void testErrorsInResults() {

        //
        // given
        //
        Map map = new HashMap();
        map.put("_id", "foo");
        SinkRecord sr1 = new SinkRecord("test", 13, null, "0001", null, map, 0);
        SinkRecord sr2 = new SinkRecord("test", 13, null, "0002", null, map, 0);

        List<DocumentResult> documentResults = new LinkedList<>();

        Cloudant mockCloudant = PowerMock.createMock(Cloudant.class);
        Ok mockOk = PowerMock.createMock(Ok.class);
        ErrantRecordReporter mockRecordReporter = mock(ErrantRecordReporter.class);
        SinkTaskContext mockContext = mock(SinkTaskContext.class);
        DocumentResult mockDocumentResult1 = mock(DocumentResult.class);
        DocumentResult mockDocumentResult2 = mock(DocumentResult.class);
        documentResults.add(mockDocumentResult1);
        documentResults.add(mockDocumentResult2);

        expect(mockContext.errantRecordReporter()).andReturn(mockRecordReporter).anyTimes();
        expect(mockRecordReporter.report(eq(sr1), anyObject(RuntimeException.class))).andReturn(null);
        expect(mockCloudant.postBulkDocs(anyObject())).andReturn(ServiceCallUtils.makeServiceCallWithResult(documentResults)).anyTimes();
        expect(mockCloudant.putDatabase(anyObject())).andReturn(ServiceCallUtils.makeServiceCallWithResult(mockOk)).anyTimes();
        // first document result is bad, second is good
        expect(mockDocumentResult1.isOk()).andReturn(null).anyTimes(); // NB service may return null or false if not ok
        expect(mockDocumentResult1.getError()).andReturn("some error").anyTimes();
        expect(mockDocumentResult1.getReason()).andReturn("some cause").anyTimes();
        expect(mockDocumentResult2.isOk()).andReturn(Boolean.TRUE).anyTimes();

        // force the task to use our mock client
        ClientManagerUtils.addClientToCache(connectionName, mockCloudant);
        // setup task with some minimal config
        SinkTask sinkTask = new SinkTask();

        Map<String, String> configMap = new HashMap<>();
        configMap.put("name", connectionName);
        configMap.put("cloudant.since", "1000");
        configMap.put("cloudant.url", "http://foo");
        configMap.put("cloudant.db", "foo");
        configMap.put("topics", "foo");

        replay(mockCloudant);
        replay(mockOk);
        replay(mockRecordReporter);
        replay(mockContext);
        replay(mockDocumentResult1);
        replay(mockDocumentResult2);

        //
        // when
        //
        sinkTask.initialize(mockContext);
        sinkTask.start(configMap);
        sinkTask.put(Collections.singleton(sr1));
        sinkTask.put(Collections.singleton(sr2));
        sinkTask.flush(new HashMap<>());

        //
        // then
        //
        EasyMock.verify(mockRecordReporter);
    }

    // verify that throwing an exception causes connect exception to be thrown from flush
    @Test
    public void testExceptionWhenCallingPostBulkDocs() {

        //
        // given
        //
        Map map = new HashMap();
        map.put("_id", "foo");
        SinkRecord sr = new SinkRecord("test", 13, null, "0001", null, map, 0);
        Exception exception = new RuntimeException("eek!");

        Cloudant mockCloudant = PowerMock.createMock(Cloudant.class);
        Ok mockOk = PowerMock.createMock(Ok.class);
        ErrantRecordReporter mockRecordReporter = mock(ErrantRecordReporter.class);
        SinkTaskContext mockContext = mock(SinkTaskContext.class);

        expect(mockContext.errantRecordReporter()).andReturn(mockRecordReporter).anyTimes();
        expect(mockCloudant.postBulkDocs(anyObject())).andThrow(exception).anyTimes();
        expect(mockCloudant.putDatabase(anyObject())).andReturn(ServiceCallUtils.makeServiceCallWithResult(mockOk)).anyTimes();

        // force the task to use our mock client
        ClientManagerUtils.addClientToCache(connectionName, mockCloudant);
        // setup task with some minimal config
        SinkTask sinkTask = new SinkTask();

        Map<String, String> configMap = new HashMap<>();
        configMap.put("name", connectionName);
        configMap.put("cloudant.since", "1000");
        configMap.put("cloudant.url", "http://foo");
        configMap.put("cloudant.db", "foo");
        configMap.put("topics", "foo");

        replay(mockCloudant);
        replay(mockOk);
        replay(mockRecordReporter);
        replay(mockContext);

        //
        // when
        //
        sinkTask.initialize(mockContext);
        sinkTask.start(configMap);
        sinkTask.put(Collections.singleton(sr));

        //
        // then
        //
        try {
            sinkTask.flush(new HashMap<>());
            Assert.fail("ConnectException not thrown");
        } catch (ConnectException connectException) {
            Assert.assertEquals("Exception thrown when trying to write documents", connectException.getMessage());
        }
        // no expects for this - should never be called
        verify(mockRecordReporter);
    }

    @After
    public void teardown() {
        PowerMock.resetAll();
    }
}
