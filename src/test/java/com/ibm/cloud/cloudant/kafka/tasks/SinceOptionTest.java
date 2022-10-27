package com.ibm.cloud.cloudant.kafka.tasks;

import com.ibm.cloud.cloudant.kafka.caching.ClientManagerUtils;
import com.ibm.cloud.cloudant.kafka.utils.InterfaceConst;
import com.ibm.cloud.cloudant.kafka.utils.ServiceCallUtils;
import com.ibm.cloud.cloudant.v1.Cloudant;
import com.ibm.cloud.cloudant.v1.model.Change;
import com.ibm.cloud.cloudant.v1.model.ChangesResult;
import com.ibm.cloud.cloudant.v1.model.ChangesResultItem;
import com.ibm.cloud.cloudant.v1.model.Document;
import com.ibm.cloud.cloudant.v1.model.PostChangesOptions;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.junit.Assert;
import org.junit.Test;
import org.powermock.api.easymock.PowerMock;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.ibm.cloud.cloudant.kafka.tasks.ConnectorConfig.LAST_SEQ_NUM_DEFAULT;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;

public class SinceOptionTest {

    private static final String CONNECTION_NAME = "_mock";

    private static final String DB_NAME = "foo";

    // last_seq, as stored in offset storage
    private static final String LAST_SEQ = "123-abc-xyz";

    // *next* last_seq, as returned in changes
    private static final String NEXT_LAST_SEQ = "456-def-rst";

    // cloudant.since, as requested in config
    private static final String CLOUDANT_SINCE = "now";

    // document id
    private static final String ID = "123";

    // base options for config
    private static final Map<String, String> CONFIG_MAP = new HashMap<>();

    static {
        CONFIG_MAP.put("name", CONNECTION_NAME);
        CONFIG_MAP.put("cloudant.url", "http://foo");
        CONFIG_MAP.put("cloudant.db", DB_NAME);
        CONFIG_MAP.put("topics", "foo");
    }

    // test offset storage reader overrides option from config map:
    // - configure cloudant.since to CLOUDANT_SINCE in config map
    // - configure offset storage reader to return LAST_SEQ
    // - expect LAST_SEQ to be used in _changes request, which is value from offset storage reader
    @Test
    public void testOffsetStorageReaderValueOverridesOption() throws Exception {
        Map<String, String> configMap = new HashMap<>(CONFIG_MAP);
        configMap.put("cloudant.since", CLOUDANT_SINCE);
        testOffsetStorageReaderBehaviour(Collections.singletonMap(InterfaceConst.LAST_CHANGE_SEQ, LAST_SEQ), configMap, LAST_SEQ);
    }

    // test no offset storage reader value, value from config map is used:
    // - configure cloudant.since to CLOUDANT_SINCE in config map
    // - configure offset storage reader to return null
    // - expect CLOUDANT_SINCE to be used in _changes request, which is value from offset storage reader
    @Test
    public void testNoOffsetStorageReaderValueAndOptionSpecified() throws Exception {
        Map<String, String> configMap = new HashMap<>(CONFIG_MAP);
        configMap.put("cloudant.since", CLOUDANT_SINCE);
        testOffsetStorageReaderBehaviour(null, configMap, CLOUDANT_SINCE);
    }

    // test no offset storage reader value, value from config map is used:
    // - don't configure cloudant.since in config map
    // - configure offset storage reader to return null
    // - expect "0" (LAST_SEQ_NUM_DEFAULT) to be used in _changes request, which is value from offset storage reader
    @Test
    public void testNoOffsetStorageReaderValueAndNoOptionSpecified() throws Exception {
        Map<String, String> configMap = new HashMap<>(CONFIG_MAP);
        testOffsetStorageReaderBehaviour(null, configMap, LAST_SEQ_NUM_DEFAULT);
    }

    // helper method, test offset storage reader behaviour parameterised on:
    // - offset returned from offset storage reader
    // - config map on task
    // - expected "since" value in call to _changes
    private void testOffsetStorageReaderBehaviour(Map<String, Object> offsetFromOffsetStorageReader,
                                                 Map<String, String> configMap,
                                                 String sinceValueExpectedInRequest) throws Exception {
        //
        // given...
        //
        Cloudant mockCloudant = PowerMock.createMock(Cloudant.class);
        ChangesResult mockChangesResult = PowerMock.createMock(ChangesResult.class);
        ChangesResultItem mockChangesResultItem = PowerMock.createMock(ChangesResultItem.class);
        Change mockChange = PowerMock.createMock(Change.class);
        SourceTaskContext mockContext = PowerMock.createMock(SourceTaskContext.class);
        OffsetStorageReader mockOsr = PowerMock.createMock(OffsetStorageReader.class);
        // dummy document to return, we don't care about contents
        Document document = new Document();
        document.setId(ID);
        // these options are used in the `expect` call the line below to ensure that we are matching the correct "since" value
        PostChangesOptions options = new PostChangesOptions.Builder()
                .db(DB_NAME)
                .includeDocs(true)
                .since(sinceValueExpectedInRequest)
                .limit(InterfaceConst.DEFAULT_BATCH_SIZE_SOURCE)
                .build();
        expect(mockCloudant.postChanges(options)).andReturn(ServiceCallUtils.makeServiceCallWithResult(mockChangesResult)).anyTimes();
        expect(mockContext.offsetStorageReader()).andReturn(mockOsr);
        // return the offset given as an argument to us
        expect(mockOsr.offset(anyObject())).andReturn(offsetFromOffsetStorageReader);
        expect(mockChangesResult.getResults()).andReturn(Collections.singletonList(mockChangesResultItem)).times(2);
        expect(mockChangesResult.getLastSeq()).andReturn(NEXT_LAST_SEQ);
        expect(mockChangesResultItem.getChanges()).andReturn(Collections.singletonList(mockChange));
        expect(mockChangesResultItem.getSeq()).andReturn(NEXT_LAST_SEQ);
        expect(mockChangesResultItem.getId()).andReturn(ID);
        expect(mockChangesResultItem.getDoc()).andReturn(document);
        expect(mockChangesResultItem.isDeleted()).andReturn(null);
        // force the task to use our mock client
        ClientManagerUtils.addClientToCache(CONNECTION_NAME, mockCloudant);
        SourceChangesTask sourceChangesTask = new SourceChangesTask();
        sourceChangesTask.initialize(mockContext);

        replay(mockCloudant);
        replay(mockChangesResult);
        replay(mockChange);
        replay(mockChangesResultItem);
        replay(mockContext);
        replay(mockOsr);

        //
        // when...
        //
        sourceChangesTask.start(configMap);
        List<SourceRecord> srs = sourceChangesTask.poll();

        //
        // then...
        //
        // NB this assertion is only here as an "extra" - most of the work has already done by the "expect" calls
        Assert.assertEquals(NEXT_LAST_SEQ, srs.get(0).sourceOffset().get(InterfaceConst.LAST_CHANGE_SEQ));
    }

}
