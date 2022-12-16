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

package com.ibm.cloud.cloudant.kafka.tasks;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.ibm.cloud.cloudant.kafka.caching.CachedClientManager;
import com.ibm.cloud.cloudant.kafka.utils.InterfaceConst;
import com.ibm.cloud.cloudant.kafka.utils.JavaCloudantUtil;
import com.ibm.cloud.cloudant.v1.Cloudant;
import com.ibm.cloud.sdk.core.http.ServiceCall;

public abstract class AbstractSourceTask<R, S> extends org.apache.kafka.connect.source.SourceTask {

    protected final Logger LOG = LoggerFactory.getLogger(this.getClass());
    protected final List<String> offsetKeys;
    protected Cloudant service;
    protected AbstractConfig config;
    protected String url = null;
    protected String db = null;
    protected List<String> topics = null;
    protected Map<String, Object> offset;
    protected Map<String, String> sourcePartition;
    protected BiFunction<String, S, Stream<SourceRecord>> itemToSourceRecord;

    AbstractSourceTask(List<String> offsetKeys) {
        this.offsetKeys = offsetKeys;
    }

    @Override
    public String version() {
        return JavaCloudantUtil.VERSION;
    }

    @Override
    public void start(Map<String, String> props) {
        this.config = new SourceChangesConnectorConfig(SourceChangesConnectorConfig.CONFIG_DEF, props);
        this.url = this.config.getString(InterfaceConst.URL);
        this.db = this.config.getString(InterfaceConst.DB);
        this.topics = this.config.getList(InterfaceConst.TOPIC);

        this.service = CachedClientManager.getInstance(config.originalsStrings());

        setSourceParition();
        
        // Read a configured or default offset(s) from config
        this.offset = new HashMap<>();
        offsetKeys.forEach((k) -> {
            this.offset.put(k, config.getString(k));
        });

        // check for a stored offset and if available use it in preference to the default or user-supplied option
        OffsetStorageReader offsetReader = context.offsetStorageReader();
        if (offsetReader != null) {
            Map<String, Object> offset = offsetReader.offset(this.sourcePartition);
            if (offset != null) {
                LOG.info("Retrieved offset from OffsetStorageReader");
                this.offset = new HashMap<>(offset);
            }
        }
        this.offset.forEach( (k, v) -> {
            LOG.info("Start with {}={}", k, v);
        });

        // Only now set the record function (in case it wants to use partition or offset)
        this.itemToSourceRecord = getItemToRecordFunction();
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        LOG.debug("Polling from offsets:\n {}", this.offset);
        ServiceCall<R> call = nextServiceCall();
        R result = call.execute().getResult();
        if (result != null) {
            List<SourceRecord> records = resultToStream(result)
            .flatMap(item -> topics.stream().flatMap(topic -> {
                return itemToSourceRecord.apply(topic, item);
            })).collect(Collectors.toList());
            // Update offset
            updateOffset(result, records);
            LOG.debug("Return {} records with last offset {}", records.size(), offset);
            return records;
        }
        return null;
    }

    @Override
    public void stop() {
        // Default implementation, no-op
    }
    
    protected void setSourceParition() {
        this.sourcePartition = new HashMap<>();
        // Default to a Map of URL and DB, but can be overridden
        this.sourcePartition.put(InterfaceConst.URL, this.url);
        this.sourcePartition.put(InterfaceConst.DB, this.db);
    }

    protected abstract BiFunction<String, S, Stream<SourceRecord>> getItemToRecordFunction();

    protected abstract ServiceCall<R> nextServiceCall();

    protected abstract Stream<S> resultToStream(R result);

    protected abstract void updateOffset(R result, List<SourceRecord> records);

}
