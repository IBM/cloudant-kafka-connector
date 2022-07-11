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
package com.ibm.cloudant.kafka.transforms;

import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SchemaUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

import static org.apache.kafka.connect.transforms.util.Requirements.*;

public class KeyToDocId<R extends ConnectRecord<R>> implements Transformation<R> {

    public static final String OVERVIEW_DOC = "Extract the record's key and save the value to the '_id' field. " +
            "If the '_id' record already exists, the value will be overwritten." +
            "If the record key doesn't exist, the record will be unmodified.";

    public static final ConfigDef CONFIG_DEF = new ConfigDef();

    private static final String PURPOSE = "Copy record key to _id value";

    private static final String idFieldName = "_id";

    private Cache<Schema, Schema> schemaUpdateCache = new SynchronizedCache<>(new LRUCache<Schema, Schema>(16));

    private static Logger LOG = LoggerFactory.getLogger(KeyToDocId.class.toString());

    public void configure(Map<String, ?> configs) {
        // no config required
    }

    public R apply(R record) {
        if (record.key() != null
                && String.valueOf(record.key()).length() > 0) {
            LOG.info("Attempting to apply KeyToDocId transform");
            if (record.valueSchema() == null) {
                return applySchemaless(record);
            } else {
                return applyWithSchema(record);
            }
        } else {
            LOG.info("The topic message does not have a key. Leaving message unmodified.");
           return record;
        }
    }

    private R applySchemaless(R record) {
        final Map<String, Object> value;
        if (record.value() == null) {
            value = new HashMap<>(1);
        } else {
            value = requireMapOrNull(record.value(), PURPOSE);
        }
        final Map<String, Object> updatedValue = new HashMap<>(value);
        String keyToUseAsId = String.valueOf(record.key());
        if (keyToUseAsId != null) {
            updatedValue.put(idFieldName, keyToUseAsId);
        } else {
            LOG.info("The topic message does not have a key. Leaving message unmodified.");
        }
        return record.newRecord(
                record.topic(),
                record.kafkaPartition(),
                record.keySchema(),
                record.key(),
                record.valueSchema(),
                updatedValue,
                record.timestamp()
        );
    }

    private R applyWithSchema(R record) {
        final Struct value = requireStruct(record.value(), PURPOSE);

        Schema updatedSchema = schemaUpdateCache.get(value.schema());
        if (updatedSchema == null) {
            updatedSchema = makeUpdatedSchema(value.schema());
            schemaUpdateCache.put(value.schema(), updatedSchema);
        }

        final Struct updatedValue = new Struct(updatedSchema);
        for (Field field : value.schema().fields()) {
            updatedValue.put(field.name(), value.get(field));
        }

        String keyToUseAsId = String.valueOf(record.key());
        if (!keyToUseAsId.equals("Struct{}")) {
            try {
                updatedValue.put(idFieldName, keyToUseAsId);
            } catch (DataException de) {
                LOG.info("The _id field does not exist in the message value schema. Leaving message unmodified.");
            }
        }

        return record.newRecord(
                record.topic(),
                record.kafkaPartition(),
                record.keySchema(),
                record.key(),
                updatedSchema,
                updatedValue,
                record.timestamp()
        );
    }

    private Schema makeUpdatedSchema(Schema schema) {
        final SchemaBuilder builder = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct());
        for (Field field: schema.fields()) {
            builder.field(field.name(), field.schema());
        }

        return builder.build();
    }

    public ConfigDef config() {
        return CONFIG_DEF;
    }

    public void close() {
        schemaUpdateCache = null;
    }
}
