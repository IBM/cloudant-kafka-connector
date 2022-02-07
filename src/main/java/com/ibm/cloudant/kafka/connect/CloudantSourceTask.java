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
package com.ibm.cloudant.kafka.connect;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.ibm.cloud.cloudant.v1.Cloudant;
import com.ibm.cloud.cloudant.v1.model.ChangesResult;
import com.ibm.cloud.cloudant.v1.model.ChangesResultItem;
import com.ibm.cloud.cloudant.v1.model.Document;
import com.ibm.cloud.cloudant.v1.model.PostChangesOptions;
import com.ibm.cloudant.kafka.common.InterfaceConst;
import com.ibm.cloudant.kafka.common.MessageKey;
import com.ibm.cloudant.kafka.common.utils.JavaCloudantUtil;
import com.ibm.cloudant.kafka.common.utils.ResourceBundleUtil;
import com.ibm.cloudant.kafka.schema.DocumentAsSchemaStruct;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.apache.log4j.Logger;

import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class CloudantSourceTask extends SourceTask {

    private static Logger LOG = Logger.getLogger(CloudantSourceTask.class);

    private static final long FEED_SLEEP_MILLISEC = 5000;
    private static final long SHUTDOWN_DELAY_MILLISEC = 10;
    private static final String DEFAULT_CLOUDANT_LAST_SEQ = "0";

    private AtomicBoolean stop;
    private AtomicBoolean _running;

    private String url = null;
    private List<String> topics = null;
    private boolean generateStructSchema = false;
    private boolean flattenStructSchema = false;
    private boolean omitDesignDocs = false;

    private String latestSequenceNumber = null;
    private int batch_size = 0;

    private Cloudant service = null;
    private ChangesResult cloudantChangesResult = null;

    @Override
    public List<SourceRecord> poll() throws InterruptedException {


        // stop will be set but can be honored only after we release
        // the changes() feed
        while (!stop.get()) {
            _running.set(true);

            // the array to be returned
            ArrayList<SourceRecord> records = new ArrayList<SourceRecord>();

            LOG.debug("Process lastSeq: " + latestSequenceNumber);

            // the changes feed for initial processing (not continuous yet)
            PostChangesOptions postChangesOptions = new PostChangesOptions.Builder()
                .db(JavaCloudantUtil.getDbNameFromUrl(url))
                .includeDocs(true)
                .since(latestSequenceNumber)
                .limit(batch_size)
                .build();
            cloudantChangesResult = service.postChanges(postChangesOptions).execute().getResult();

            if (cloudantChangesResult != null) {
                LOG.debug("Got " + cloudantChangesResult.getResults().size() + " changes");
                latestSequenceNumber = cloudantChangesResult.getLastSeq();

                // process the results into the array to be returned
                for (ChangesResultItem row : cloudantChangesResult.getResults()) {
                    Document doc = row.getDoc();
                    Schema docSchema;
                    Object docValue;
                    if (generateStructSchema) {
                        Struct docStruct = DocumentAsSchemaStruct.convert(
                            new Gson().fromJson(doc.toString(), JsonObject.class),
                            flattenStructSchema);
                        docSchema = docStruct.schema();
                        docValue = docStruct;
                    } else {
                        docSchema = Schema.STRING_SCHEMA;
                        docValue = doc.toString();
                    }

                    String id = row.getId();
                    if (!omitDesignDocs || !id.startsWith("_design/")) {
                        // Emit the record to every topic configured
                        for (String topic : topics) {
                            SourceRecord sourceRecord = new SourceRecord(offsetKey(url),
                                    offsetValue(latestSequenceNumber),
                                    topic, // topics
                                    //Integer.valueOf(row_.getId())%3, // partition
                                    Schema.STRING_SCHEMA, // key schema
                                    id, // key
                                    docSchema, // value schema
                                    docValue); // value
                            records.add(sourceRecord);
                        }
                    }
                }

                LOG.info("Return " + records.size() / topics.size() + " records with last offset "
                        + latestSequenceNumber);

                cloudantChangesResult = null;

                _running.set(false);
                return records;
            }
        }

        // Only in case of shutdown
        return null;
    }


    @Override
    public void start(Map<String, String> props) {

        try {
            CloudantSourceTaskConfig config = new CloudantSourceTaskConfig(props);

            url = config.getString(InterfaceConst.URL);
            String userName = config.getString(InterfaceConst.USER_NAME);
            String password = config.getPassword(InterfaceConst.PASSWORD).value();
            service = JavaCloudantUtil.getClientInstance(url, userName, password);
            topics = config.getList(InterfaceConst.TOPIC);
            omitDesignDocs = config.getBoolean(InterfaceConst.OMIT_DESIGN_DOCS);
            generateStructSchema = config.getBoolean(InterfaceConst.USE_VALUE_SCHEMA_STRUCT);
            flattenStructSchema = config.getBoolean(InterfaceConst.FLATTEN_VALUE_SCHEMA_STRUCT);

            latestSequenceNumber = config.getString(InterfaceConst.LAST_CHANGE_SEQ);
            batch_size = config.getInt(InterfaceConst.BATCH_SIZE) == null ? InterfaceConst
                    .DEFAULT_BATCH_SIZE : config.getInt(InterfaceConst.BATCH_SIZE);
				
			/*if (tasks_max > 1) {
				throw new ConfigException("CouchDB _changes API only supports 1 thread. Configure
				tasks.max=1");
			}*/

            _running = new AtomicBoolean(false);
            stop = new AtomicBoolean(false);

            if (latestSequenceNumber == null) {
                latestSequenceNumber = DEFAULT_CLOUDANT_LAST_SEQ;

                OffsetStorageReader offsetReader = context.offsetStorageReader();

                if (offsetReader != null) {
                    Map<String, Object> offset = offsetReader.offset(Collections.singletonMap
                            (InterfaceConst.URL, url));
                    if (offset != null) {
                        latestSequenceNumber = (String) offset.get(InterfaceConst.LAST_CHANGE_SEQ);
                        LOG.info("Start with current offset (last sequence): " +
                                latestSequenceNumber);
                    }
                }
            }

        } catch (ConfigException | MalformedURLException e) {
            // TODO remove this catch block to throw config exceptions when properties don't exist
            throw new ConnectException(ResourceBundleUtil.get(MessageKey.CONFIGURATION_EXCEPTION)
                    , e);
        }

    }

    @Override
    public void stop() {
        // TODO needs a custom changes feed
        if (stop != null) {
            stop.set(true);
        }

        // terminate the changes feed

        // graceful shutdown
        // allow the poll() method to flush records first
        if (_running == null) {
            return;
        }

        while (_running.get()) {
            try {
                Thread.sleep(SHUTDOWN_DELAY_MILLISEC);
            } catch (InterruptedException e) {
                LOG.error(e);
            }
        }
    }


    private Map<String, String> offsetKey(String url) {
        return Collections.singletonMap(InterfaceConst.URL, url);
    }

    private Map<String, String> offsetValue(String lastSeqNumber) {
        return Collections.singletonMap(InterfaceConst.LAST_CHANGE_SEQ, lastSeqNumber);
    }

    public String version() {
        return new CloudantSourceConnector().version();
    }

}
