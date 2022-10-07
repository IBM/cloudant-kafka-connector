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
package com.ibm.cloud.cloudant.kafka.mappers;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.source.SourceRecord;
import com.ibm.cloud.cloudant.kafka.utils.CloudantConst;
import com.ibm.cloud.cloudant.v1.model.ChangesResultItem;
import com.ibm.cloud.cloudant.v1.model.Document;

public class DocumentToSourceRecord implements BiFunction<String, ChangesResultItem, SourceRecord> {

    // Record key schema is Map<String, String>
    private static final Schema RECORD_KEY_SCHEMA = SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA).build();
    // Record value is Map<String, Object>
    // We can't use map(Schema.STRING_SCHEMA, null) as no schema (null) is not permitted for Kafka Connect's Map schema values.
    // So we just use a `null` and the map should be inferred from its Java class.
    private static final Schema RECORD_VALUE_SCHEMA = null;

    enum MetaProperty {

        ATTACHMENTS("_attachments", Document::getAttachments),
        CONFLICTS("_conflicts", Document::getConflicts),
        DELETED("_deleted", Document::isDeleted),
        DELETED_CONFLICTS("_deleted_conflicts", Document::getDeletedConflicts),
        ID("_id", Document::getId),
        LOCAL_SEQ("_local_seq", Document::getLocalSeq),
        REV("_rev", Document::getRev),
        REVISIONS("_revisions", Document::getRevisions),
        REVS_INFO("_revs_info", Document::getRevsInfo);

        private final String propertyName;
        private final Function<Document, ?> getter;
        
        MetaProperty(String propertyName, Function<Document, ?> getter) {
            this.propertyName = propertyName;
            this.getter = getter;
        }

        Object getValueFromDoc(Document d) {
            return getter.apply(d);
        }

        @Override
        public String toString() {
            return this.propertyName;
        }
    }

    private final Map<String, String> partition;
    private final Function<String, Map<String, String>> offsetFunction;

    public DocumentToSourceRecord(Map<String, String> partition, Function<String, Map<String, String>> offsetFunction) {
        this.partition = partition;
        this.offsetFunction = offsetFunction;
    }

    @Override
    public SourceRecord apply(String topic, ChangesResultItem changesResultItem) {
        return new SourceRecord(partition, 
        offsetFunction.apply(changesResultItem.getSeq()),
        topic,
        RECORD_KEY_SCHEMA,
        Collections.singletonMap(CloudantConst.CLOUDANT_DOC_ID, changesResultItem.getId()),
        RECORD_VALUE_SCHEMA,
        documentToMap(changesResultItem.getDoc()));
    }

    Map<String, Object> documentToMap(Document document) {
        Map<String, Object> fixedProperties = getFixedProperties(document);
        Map<String, Object> dynamicProperties = Collections.unmodifiableMap(document.getProperties());
        Map<String, Object> map = new HashMap<>(fixedProperties.size() + dynamicProperties.size());
        map.putAll(fixedProperties);
        map.putAll(dynamicProperties);
        return Collections.unmodifiableMap(map);
    }

    private final Map<String, Object> getFixedProperties(Document document) {
        Map<String, Object> map = new HashMap<>(MetaProperty.values().length);
        for (MetaProperty metaProp : MetaProperty.values()) {
            Object value = metaProp.getValueFromDoc(document);
            if (value != null) {
                map.put(metaProp.toString(), value);
            }
        }
        return Collections.unmodifiableMap(map);
    }
}
