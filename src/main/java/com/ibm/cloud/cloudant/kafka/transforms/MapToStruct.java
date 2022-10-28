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
package com.ibm.cloud.cloudant.kafka.transforms;

import java.math.BigDecimal;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.transforms.Transformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.ibm.cloud.cloudant.kafka.utils.MessageKey;
import com.ibm.cloud.cloudant.kafka.utils.ResourceBundleUtil;
import static org.apache.kafka.connect.transforms.util.Requirements.requireMapOrNull;

public class MapToStruct implements Transformation<SourceRecord> {

    public static final Schema NULL_VALUE_SCHEMA = SchemaBuilder.string().optional().name("NULL_VALUE_SCHEMA").build();
    private static final SchemaAndValue NULL_SCHEMA_AND_VALUE = new SchemaAndValue(NULL_VALUE_SCHEMA, null);
    private static final ConfigDef EMPTY_CONFIG = new ConfigDef();
    private static final Logger LOG = LoggerFactory.getLogger(ArrayFlatten.class);

    @Override
    public void configure(Map<String, ?> configs) {
        // No-op no configuration to apply
    }

    @Override
    @SuppressWarnings("unchecked")
    public SourceRecord apply(SourceRecord record) {
        try {
            requireMapOrNull(record.value(), String.format(ResourceBundleUtil.get(MessageKey.CLOUDANT_STRUCT_PURPOSE), this.getClass().getName()));
        } catch (DataException de) {
            LOG.warn(ResourceBundleUtil.get(MessageKey.CLOUDANT_TRANSFORM_FILTER_RECORD), de);
        }
        if (record.value() != null) {
            Map<String, Object> originalValueAsMap = (Map<String, Object>) record.value();
            SchemaAndValue transformed = mapToStruct(originalValueAsMap);
            return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), transformed.schema(), transformed.value(), record.timestamp());
        }
        // If there is a null value we can't transform so return the original record
        return record;
    }

    @Override
    public ConfigDef config() {
        // No configuration available for this transform
        return EMPTY_CONFIG;
    }

    @Override
    public void close() {
        // No-op, no cleanup to do.
    }

    static SchemaAndValue mapToStruct(Map<String, Object> jsonMap) {
        // Collect in a TreeMap so the keys are ordered
        Map<String, SchemaAndValue> intermediate = jsonMap.entrySet().stream().parallel()
            .collect(Collectors.toMap(Map.Entry::getKey, MapToStruct::entryToSchemaAndValue, (v1, v2) -> v1, TreeMap::new));
        SchemaBuilder schemaBuilder = SchemaBuilder.struct().optional();
        intermediate.forEach((k, sav) -> {
            schemaBuilder.field(k, sav.schema());
        });
        Schema schema = schemaBuilder.build();
        Struct struct = new Struct(schema);
        intermediate.forEach((k, sav) -> {
            struct.put(k, sav.value());
        });
        return new SchemaAndValue(schema, struct);
    }

    static SchemaAndValue listToArray(List<Object> jsonArray) {
        List<SchemaAndValue> array = jsonArray.stream()
            .sequential()
            .map(MapToStruct::objectToSchemaAndValue)
            .collect(Collectors.toList());
        List<Schema> schemas = array.stream().map(SchemaAndValue::schema).distinct().collect(Collectors.toList());
        boolean hasNullValues = schemas.remove(NULL_VALUE_SCHEMA);
        Schema arraySchema;
        if( array.isEmpty() || (hasNullValues && schemas.isEmpty())) {
            // There are certain arrays where we cannot infer the schema from the elements.
            // If there are no elements to test, or if the only values are nulls.
            // For these cases we just call it an optional string and move on.
            arraySchema = NULL_VALUE_SCHEMA;
            LOG.warn(ResourceBundleUtil.get(MessageKey.CLOUDANT_STRUCT_UNDETECTABLE_ARRAY));
        } else if (schemas.size() > 1) {
            // If there are multiple schemas then throw an exception because mixed type arrays are not supported by Struct
            throw new DataException(String.format(ResourceBundleUtil.get(MessageKey.CLOUDANT_STRUCT_MIXED_TYPE_ARRAY), ArrayFlatten.class.getName()));
        } else {
            // Finally for the case of a single schema for all elements we can use it
            arraySchema = schemas.get(0);
        }
        return new SchemaAndValue(SchemaBuilder.array(arraySchema).optional().build(), array.stream().map(SchemaAndValue::value).collect(Collectors.toList()));
    }

    @SuppressWarnings("unchecked")
    static SchemaAndValue objectToSchemaAndValue(Object o) {
        if (o == null) {
            return NULL_SCHEMA_AND_VALUE;
        } else {
            Schema.Type inferredSchemaType = ConnectSchema.schemaType(o.getClass());
            if (inferredSchemaType != null) {
                switch(inferredSchemaType) {
                    case ARRAY:
                        return listToArray((List<Object>) o);
                    case MAP:
                        return mapToStruct((Map<String, Object>) o);
                    // Primitives
                    case BOOLEAN:
                    case BYTES:
                    case FLOAT32:
                    case FLOAT64:
                    case INT16:
                    case INT32:
                    case INT64:
                    case INT8:
                    case STRING:
                        return new SchemaAndValue(new SchemaBuilder(inferredSchemaType).optional().build(), o);
                    case STRUCT:
                    default:
                        throw new DataException(String.format(ResourceBundleUtil.get(MessageKey.CLOUDANT_STRUCT_UNHANDLED_TYPE), inferredSchemaType.name()));
                }
            } else {
                // Inferred schema was null, could be one of the other Connect data types
                // Decimal
                // Date/Time/Timestamp
                if (o instanceof BigDecimal) {
                    BigDecimal bd = (BigDecimal) o;
                    Schema s = Decimal.builder(bd.scale()).optional().build();
                    return new SchemaAndValue(s, o);
                } else if (o instanceof Date) {
                    // Could also be a Time or a Date, but Timestamp keeps both parts.
                    Schema s = Timestamp.builder().optional().build();
                    return new SchemaAndValue(s, o);
                }
                throw new DataException(String.format(ResourceBundleUtil.get(MessageKey.CLOUDANT_STRUCT_UNKNOWN_TYPE), o.getClass().getName()));
            }
        }
    }

    static SchemaAndValue entryToSchemaAndValue(Map.Entry<String, Object> entry) {
        return objectToSchemaAndValue(entry.getValue());
    }
}
