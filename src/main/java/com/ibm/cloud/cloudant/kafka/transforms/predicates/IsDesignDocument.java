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
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.transforms.predicates.Predicate;
import org.apache.kafka.connect.transforms.util.Requirements;
import com.ibm.cloud.cloudant.kafka.utils.MessageKey;
import com.ibm.cloud.cloudant.kafka.utils.ResourceBundleUtil;
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
    @SuppressWarnings("unchecked")
    public boolean test(SourceRecord record) {
        Schema schema = record.keySchema();
        Object recordKey = record.key();
        if (schema == null || recordKey == null || schema.type() != Type.MAP || schema.keySchema().type() != Type.STRING || schema.valueSchema().type() != Type.STRING) {
            // Unexpected schema, check if the key is a map
            // No need to use message file here, because this purpose gets inserted into the middle of an untranslated Kafka message
            Requirements.requireMap(recordKey, "record key in IsDesignDocument predicate");
            // If it is not a map this will throw
        }
        Map<String, String> key = (Map<String, String>) recordKey;
        String id = key != null ? key.get(CLOUDANT_DOC_ID) : null;
        if (id == null) {
            throw new DataException(String.format(ResourceBundleUtil.get(MessageKey.CLOUDANT_KEY_NO_ID), CLOUDANT_DOC_ID));
        } else {
            return id.startsWith(CLOUDANT_DESIGN_PREFIX);
        }
    }

    @Override
    public void close() {
        // No-op; nothing to close
    }
    
}
