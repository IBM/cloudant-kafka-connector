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
package com.ibm.cloud.cloudant.kafka.transforms.predicates;

import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.transforms.predicates.Predicate;
import org.apache.kafka.connect.transforms.util.Requirements;

import static com.ibm.cloud.cloudant.kafka.utils.CloudantConst.CLOUDANT_DESIGN_PREFIX;
import static com.ibm.cloud.cloudant.kafka.utils.CloudantConst.CLOUDANT_DOC_ID;

public class IsDesignDocument implements Predicate<SourceRecord> {

    private static final ConfigDef EMPTY_CONFIG_DEF = new ConfigDef();

    @Override
    public void configure(Map<String, ?> configs) {
        // No-op; no configuration
    }

    @Override
    public ConfigDef config() {
        return EMPTY_CONFIG_DEF;
    }

    @Override
    public boolean test(SourceRecord record) {
        Schema schema = record.keySchema();
        Object recordKey = record.key();
        // No need to use message file here, because this purpose gets inserted into the middle of an untranslated Kafka message
        Requirements.requireSchema(schema, "record key in IsDesignDocument predicate");
        Requirements.requireStruct(recordKey, "record key in IsDesignDocument predicate");
        Struct key = (Struct) recordKey;
        // will throw DataException if not valid
        key.validate();
        String id = key.getString(CLOUDANT_DOC_ID);
        return id.startsWith(CLOUDANT_DESIGN_PREFIX);
    }

    @Override
    public void close() {
        // No-op; nothing to close
    }
    
}
