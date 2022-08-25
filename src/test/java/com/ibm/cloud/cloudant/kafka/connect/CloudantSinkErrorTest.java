package com.ibm.cloud.cloudant.kafka.connect;

import com.ibm.cloud.cloudant.kafka.connect.utils.ServiceCallUtils;
import com.ibm.cloud.cloudant.v1.Cloudant;
import com.ibm.cloud.cloudant.v1.model.ChangesResult;
import com.ibm.cloud.cloudant.v1.model.DocumentResult;
import com.ibm.cloud.cloudant.v1.model.Ok;
import com.ibm.cloud.sdk.core.http.Response;
import com.ibm.cloud.sdk.core.http.ServiceCall;
import com.ibm.cloud.sdk.core.http.ServiceCallback;
import io.reactivex.Single;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.easymock.EasyMock;
import org.easymock.Mock;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;
import org.powermock.api.easymock.PowerMock;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.replay;

public class CloudantSinkErrorTest {

    private static final String connectionName = "_mock";

    @Test
    public void testErrorsInResults() {


        Cloudant mockCloudant = PowerMock.createMock(Cloudant.class);

        List<DocumentResult> documentResults = new LinkedList<>();


//        expect(mockCloudant.postBulkDocs(anyObject())).andReturn(ServiceCallUtils.makeServiceCallWithResult());
 //       expect(mockCloudant.putDatabase(anyObject())).andReturn(ServiceCallUtils.makeServiceCallWithResult());


        CloudantSinkTask cloudantSinkTask = new CloudantSinkTask();
        ErrantRecordReporter recordReporter = mock(ErrantRecordReporter.class);
        SinkTaskContext context = mock(SinkTaskContext.class);
        expect(context.errantRecordReporter()).andReturn(recordReporter);


    }

    // verify that throwing an exception causes record reporter to be invoked
    @Test
    public void testExceptionWhenCallingPostBulkDocs() {

        Map map = new HashMap();
        map.put("_id", "foo");
        SinkRecord sr = new SinkRecord("test", 13, null, "0001", null, map, 0);
        Exception exception = new RuntimeException("eek!");

        Cloudant mockCloudant = PowerMock.createMock(Cloudant.class);
        Ok mockOk = PowerMock.createMock(Ok.class);
        ErrantRecordReporter mockRecordReporter = mock(ErrantRecordReporter.class);
        SinkTaskContext mockContext = mock(SinkTaskContext.class);

        expect(mockContext.errantRecordReporter()).andReturn(mockRecordReporter).anyTimes();
        expect(mockRecordReporter.report(sr, exception)).andReturn(null);
        expect(mockCloudant.postBulkDocs(anyObject())).andThrow(exception).anyTimes();
        expect(mockCloudant.putDatabase(anyObject())).andReturn(ServiceCallUtils.makeServiceCallWithResult(mockOk)).anyTimes();

        // force the task to use our mock client
        ClientManagerUtils.addClientToCache(connectionName, mockCloudant);
        // setup task with some minimal config
        CloudantSinkTask cloudantSinkTask = new CloudantSinkTask();
        cloudantSinkTask.setContext(mockContext);

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

        cloudantSinkTask.start(configMap);
        cloudantSinkTask.put(Collections.singleton(sr));
        cloudantSinkTask.flush(new HashMap<>());

        EasyMock.verify(mockRecordReporter);

    }
}
