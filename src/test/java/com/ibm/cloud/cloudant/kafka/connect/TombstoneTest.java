package com.ibm.cloud.cloudant.kafka.connect;

import com.ibm.cloud.cloudant.v1.Cloudant;
import com.ibm.cloud.cloudant.v1.model.Change;
import com.ibm.cloud.cloudant.v1.model.ChangesResult;
import com.ibm.cloud.cloudant.v1.model.ChangesResultItem;
import com.ibm.cloud.cloudant.v1.model.Document;
import com.ibm.cloud.sdk.core.http.Response;
import com.ibm.cloud.sdk.core.http.ServiceCall;
import com.ibm.cloud.sdk.core.http.ServiceCallback;
import io.reactivex.Single;
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

        String id = "123";

        Document document = new Document();
        document.setId(id);
        document.setDeleted(Boolean.TRUE);

        ChangesResultItem changesResultItem = PowerMock.createMock(ChangesResultItem.class);
        Change change = PowerMock.createMock(Change.class);
        Cloudant mockCloudant = PowerMock.createMock(Cloudant.class);
        Response<ChangesResult> mockResponse = PowerMock.createMock(Response.class);
        ChangesResult mockResult = PowerMock.createMock(ChangesResult.class);
        expect(mockResponse.getResult()).andReturn(mockResult);
        expect(mockResult.getResults()).andReturn(Collections.singletonList(changesResultItem));
        expect(mockResult.getResults()).andReturn(Collections.singletonList(changesResultItem));
        expect(mockResult.getLastSeq()).andReturn("100");
        expect(changesResultItem.getChanges()).andReturn(Collections.singletonList(change));
        expect(changesResultItem.getDoc()).andReturn(document);
        expect(changesResultItem.getId()).andReturn(id);
        expect(mockCloudant.postChanges(anyObject())).andReturn(new ServiceCall<ChangesResult>() {
            @Override
            public ServiceCall<ChangesResult> addHeader(String s, String s1) {
                return null;
            }

            @Override
            public Response<ChangesResult> execute() throws RuntimeException {
                return mockResponse;
            }

            @Override
            public void enqueue(ServiceCallback<ChangesResult> serviceCallback) {

            }

            @Override
            public Single<Response<ChangesResult>> reactiveRequest() {
                return null;
            }

            @Override
            public void cancel() {

            }
        });
        ClientManagerUtils.addClientToCache(connectionName, mockCloudant);
        CloudantSourceTask cloudantSourceTask = new CloudantSourceTask();
        Map<String, String> configMap = new HashMap<>();
        configMap.put("name", connectionName);
        configMap.put("cloudant.since", "1000");
        configMap.put("cloudant.url", "http://foo");
        configMap.put("cloudant.db", "foo");
        configMap.put("topics", "foo");
        replay(mockCloudant);
        replay(mockResponse);
        replay(mockResult);
        replay(change);
        replay(changesResultItem);
        cloudantSourceTask.start(configMap);
        List<SourceRecord> records = cloudantSourceTask.poll();
        Assert.assertEquals(2, records.size());
        Assert.assertNotNull(records.get(0).value());
        Assert.assertNull(records.get(1).value());
        // TODO unpack record json and assert on it
        Assert.assertEquals(id, records.get(0).key());
        Assert.assertEquals(id, records.get(1).key());
    }

}
