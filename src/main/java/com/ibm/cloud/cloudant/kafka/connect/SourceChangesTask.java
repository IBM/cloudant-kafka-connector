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
import com.ibm.cloud.cloudant.kafka.SourceChangesConnector;
import com.ibm.cloud.cloudant.kafka.schema.DocumentToSourceRecord;
import com.ibm.cloud.cloudant.v1.Cloudant;
import com.ibm.cloud.cloudant.v1.model.ChangesResult;
import com.ibm.cloud.cloudant.v1.model.ChangesResultItem;
import com.ibm.cloud.cloudant.v1.model.PostChangesOptions;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SourceChangesTask extends org.apache.kafka.connect.source.SourceTask {

    private static final Logger LOG = LoggerFactory.getLogger(SourceChangesTask.class);
    private static final String DEFAULT_CLOUDANT_LAST_SEQ = "0";

    private static final String OFFSET_KEY = "cloudant.url.and.db";

    SourceChangesTaskConfig config;
    private String url = null;
    private String db = null;
    private List<String> topics = null;

    private String latestSequenceNumber = null;
    private int batchSize = 0;

    private BiFunction<String, ChangesResultItem, SourceRecord> documentToSourceRecord;

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        Cloudant service = CachedClientManager.getInstance(config.originalsStrings());

        LOG.debug("Process lastSeq: " + latestSequenceNumber);

        // the changes feed for initial processing (not continuous yet)
        PostChangesOptions postChangesOptions = new PostChangesOptions.Builder()
                .db(db)
                .includeDocs(true)
                .since(latestSequenceNumber)
                .limit(batchSize)
                .build();
        ChangesResult cloudantChangesResult = service.postChanges(postChangesOptions).execute().getResult();

        if (cloudantChangesResult != null) {
            LOG.debug("Got " + cloudantChangesResult.getResults().size() + " changes");
            latestSequenceNumber = cloudantChangesResult.getLastSeq();

            // process the results into the array to be returned
            List<SourceRecord> records = cloudantChangesResult.getResults().stream()
                    .flatMap(row -> topics.stream().flatMap(topic -> {
                        SourceRecord record = documentToSourceRecord.apply(topic, row);
                        if (Optional.ofNullable(row.isDeleted()).orElse(false)) {
                            // row is deleted, produce a tombstone message from the record as well
                            SourceRecord tombstone = record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), null, null, record.timestamp());
                            return Stream.of(record, tombstone);
                        } else {
                            return Stream.of(record);
                        }
                    })).collect(Collectors.toList());

            LOG.info("Return " + records.size() / topics.size() + " records with last offset "
                    + latestSequenceNumber);

            return records;
        }

        // Only in case of shutdown
        return null;
    }

    @Override
    public void start(Map<String, String> props) {
        this.config = new SourceChangesTaskConfig(props);
        url = config.getString(InterfaceConst.URL);
        db = config.getString(InterfaceConst.DB);
        topics = config.getList(InterfaceConst.TOPIC);
        latestSequenceNumber = config.getString(InterfaceConst.LAST_CHANGE_SEQ);
        batchSize = config.getInt(InterfaceConst.BATCH_SIZE);
        this.documentToSourceRecord = new DocumentToSourceRecord(offset(url, db), SourceChangesTask::offsetValue);

        if (latestSequenceNumber == null) {
            latestSequenceNumber = DEFAULT_CLOUDANT_LAST_SEQ;

            OffsetStorageReader offsetReader = context.offsetStorageReader();

            if (offsetReader != null) {
                Map<String, Object> offset = offsetReader.offset(offset(url, db));
                if (offset != null) {
                    latestSequenceNumber = (String) offset.get(InterfaceConst.LAST_CHANGE_SEQ);
                    LOG.info("Start with current offset (last sequence): " +
                            latestSequenceNumber);
                }
            }
        }
    }

    @Override
    public void stop() {
        // nothing to do
    }

    // use the url and db name to form a unique offset key
    private Map<String, String> offset(String url, String db) {
        return Collections.singletonMap(OFFSET_KEY, String.format("%s/%s", url, db));
    }

    private static Map<String, String> offsetValue(String lastSeqNumber) {
        return Collections.singletonMap(InterfaceConst.LAST_CHANGE_SEQ, lastSeqNumber);
    }

    public String version() {
        return new SourceChangesConnector().version();
    }

}
