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
package com.ibm.cloud.cloudant.kafka.tasks;

import com.ibm.cloud.cloudant.kafka.utils.InterfaceConst;
import com.ibm.cloud.cloudant.kafka.mappers.ChangesResultItemToSourceRecord;
import com.ibm.cloud.cloudant.v1.model.ChangesResult;
import com.ibm.cloud.cloudant.v1.model.ChangesResultItem;
import com.ibm.cloud.cloudant.v1.model.PostChangesOptions;
import com.ibm.cloud.sdk.core.http.ServiceCall;
import org.apache.kafka.connect.source.SourceRecord;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.stream.Stream;

public class SourceChangesTask extends AbstractSourceTask<ChangesResult, ChangesResultItem> {

    private int batchSize = 0;

    public SourceChangesTask() {
        super(Collections.singletonList(InterfaceConst.LAST_CHANGE_SEQ));
    }

    @Override
    public void start(Map<String, String> props) {
        super.start(props);
        this.batchSize = config.getInt(InterfaceConst.BATCH_SIZE);
    }

    @Override
    protected BiFunction<String, ChangesResultItem, Stream<SourceRecord>> getItemToRecordFunction() {
        final ChangesResultItemToSourceRecord itemToRecord = new ChangesResultItemToSourceRecord(this.sourcePartition);
        return (topic, item) -> {
            SourceRecord record = itemToRecord.apply(topic, item);
            if (Optional.ofNullable(item.isDeleted()).orElse(false)) {
                // row is deleted, produce a tombstone message from the record as well
                SourceRecord tombstone = record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), null, null, record.timestamp());
                return Stream.of(record, tombstone);
            } else {
                return Stream.of(record);
            }
        };
    }

    @Override
    protected ServiceCall<ChangesResult> nextServiceCall() {
        PostChangesOptions postChangesOptions = new PostChangesOptions.Builder()
            .feed(PostChangesOptions.Feed.LONGPOLL)
            .timeout(60 * 1000)
            .db(db)
            .includeDocs(true)
            .since((String) this.offset.get(InterfaceConst.LAST_CHANGE_SEQ))
            .limit(batchSize)
            .build();
        return service.postChanges(postChangesOptions);
    }

    @Override
    protected Stream<ChangesResultItem> resultToStream(ChangesResult result) {
        return result.getResults().stream();
    }

    @Override
    protected void updateOffset(ChangesResult result, List<SourceRecord> records) {
        this.offset.put(InterfaceConst.LAST_CHANGE_SEQ,result.getLastSeq());
    }

}
