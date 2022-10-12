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

import com.ibm.cloud.cloudant.v1.model.Document;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class SinkRecordToDocument implements Function<SinkRecord, Document> {

    static final String HEADER_DOC_ID_KEY = "cloudant_doc_id";

    public Document apply(SinkRecord record) {
        Document document = new Document();
        document.setProperties(toMap(record));
        return document;
    }

    private Map<String, Object> toMap(SinkRecord record) {
        if (record.value() == null) {
            return Collections.emptyMap();
        }
        // we can convert from a struct or a map - assume a map when a value schema is not provided
        // NB arrays not supported at top level - they are not valid json
        Schema.Type schemaType = record.valueSchema() == null ? Schema.Type.MAP : record.valueSchema().type();
        Map<String, Object> toReturn = new HashMap<>();
        switch (schemaType) {
            case MAP:
                if (record.value() instanceof Map) {
                    convertMap((Map<?,?>) record.value(), toReturn);
                    break;
                } else {
                    throw new IllegalArgumentException(String.format("Type %s not supported with schema of type Map (or no schema)",
                            record.value().getClass()));
                }
            case STRUCT:
                if (record.value() instanceof Struct) {
                    convertStruct((Struct) record.value(), toReturn);
                    break;
                } else {
                    throw new IllegalArgumentException(String.format("Type %s not supported with schema of type Struct",
                            record.value().getClass()));
                }
            default:
                throw new IllegalArgumentException(String.format("Schema type %s not supported", record.valueSchema().type()));
        }
        // Check if custom header exists on the record and use the value for the document's id
        String headerValue = getHeaderForDocId(record);
        if (!toReturn.isEmpty() && headerValue != null && !headerValue.isEmpty()) {
            toReturn.put("_id", headerValue);
        }
        return toReturn;
    }

    // convert struct to map by adding key/values to passed in map, and returning it 
    private Map<String, Object> convertStruct(Struct struct, Map<String, Object> outMap) {
        Schema schema = struct.schema();

        // iterate fields and add to map
        for (Field f : schema.fields()) {
            Object value = struct.get(f);
            outMap.put(f.name(), convertItemFromStruct(f.schema().type(), value));
        }
        return outMap;
    }

    // convert kafka map to map by adding key/values to passed in map, and returning it
    private Map<String, Object> convertMap(Map<?,?> inMap, Map<String, Object> outMap) {

        // iterate over keys and add to map
        for (Object k : inMap.keySet()) {
            if (k instanceof String) {
                Object v = inMap.get(k);
                outMap.put((String) k, convertItem(v));
            } else {
                throw new IllegalArgumentException("unsupported type in map key " + k.getClass());
            }
        }
        return outMap;
    }

    // could be an array, list, or collection: convert each item in turn and return list
    private Collection<?> convertCollection(Collection<?> c) {
        return c.stream().map(this::convertItem).collect(Collectors.toList());
    }

    // helper for convertStruct
    // get field value, recursing if necessary for struct types
    private Object convertItemFromStruct(Type type, Object value) {

        switch (type) {
            // primitive types: just return value (JSON serialiser will deal with conversion later)
            case BOOLEAN:
            case BYTES:
            case FLOAT32:
            case FLOAT64:
            case INT16:
            case INT32:
            case INT64:
            case INT8:
            case STRING:
                return value;
            // map/struct cases: chain a new map onto this one, as the value, and recursively fill in its contents 
            case MAP:
                return convertMap((Map<?,?>) value, new HashMap<>());
            case STRUCT:
                return convertStruct((Struct) value, new HashMap<>());
            // array case:
            case ARRAY:
                return convertCollection((Collection<?>) value);
            default:
                throw new IllegalArgumentException("unknown type " + type);
        }

    }

    // helper for convertMap, convertCollection
    private Object convertItem(Object value) {
        if (value instanceof Map) {
            return convertMap((Map<?,?>) value, new HashMap<>());
        } else if (value instanceof Struct) {
            return convertStruct((Struct) value, new HashMap<>());
        } else if (value instanceof Collection) {
            return convertCollection((Collection<?>) value);
        } else {
            // assume that JSON serialiser knows how to deal with it
            return value;
        }
    }

    private String getHeaderForDocId(SinkRecord record) {
        Header value = record.headers().lastWithName(HEADER_DOC_ID_KEY);
        if (value != null && value.value() instanceof String) {
            return value.value().toString();
        }
        return null;
    }

}
