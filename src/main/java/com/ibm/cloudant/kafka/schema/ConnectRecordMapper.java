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
package com.ibm.cloudant.kafka.schema;

import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

public class ConnectRecordMapper<R extends ConnectRecord<R>> implements Function<ConnectRecord<R>, Map<String, Object>> {
    
    private static Logger LOG = LoggerFactory.getLogger(ConnectRecordMapper.class);

    public Map<String, Object> apply(ConnectRecord<R> record) {
        // Check if custom header exists on the record and use the value for the document's id
        String headerValue = getHeaderForDocId(record);
        // we can convert from a struct or a map - assume a map when a value schema is not provided
        Schema.Type schemaType = record.valueSchema() == null ? Schema.Type.MAP : record.valueSchema().type();
        Map<String, Object> toReturn = new HashMap<>();
        switch (schemaType) {
            case MAP:
                if (record.value() instanceof Map) {
                    if (headerValue != null) {
                        convertMap((Map) record.value(), toReturn);
                        toReturn.put("_id", headerValue);
                        return toReturn;
                    } else {
                        return convertMap((Map) record.value(), toReturn);
                    }
                } else {
                    throw new IllegalArgumentException(String.format("Type %s not supported with schema of type Map (or no schema)",
                            record.value().getClass()));
                }
            case STRUCT:
                if (record.value() instanceof Struct) {
                    if (headerValue != null) {
                        convertStruct((Struct) record.value(), toReturn);
                        toReturn.put("_id", headerValue);
                        return toReturn;
                    } else {
                        return convertStruct((Struct) record.value(), toReturn);
                    }
                } else {
                    throw new IllegalArgumentException(String.format("Type %s not supported with schema of type Struct",
                            record.value().getClass()));
                }
            default:
                throw new IllegalArgumentException(String.format("Schema type %s not supported", record.valueSchema().type()));
        }
    }

    // convert struct to map by adding key/values to passed in map, and returning it 
    private Map<String, Object> convertStruct(Struct struct, Map<String, Object> outMap) {
        Schema schema = struct.schema();
        
        // iterate fields and add to map
        for (Field f : schema.fields()) {
            Object value = struct.get(f);
            outMap.put(f.name(), getField(f.schema().type(), value));
        }
        return outMap;
    }

    // convert kafka map to map by adding key/values to passed in map, and returning it
    private Map<String, Object> convertMap(Map inMap, Map<String, Object> outMap) {

        for (Object k : inMap.keySet()) {
            if (k instanceof String) {
                Object v = inMap.get(k);
                if (v instanceof Map) {
                    outMap.put((String)k, convertMap((Map)v, new HashMap<>()));
                } else if (v instanceof Struct) {
                    outMap.put((String)k, convertStruct((Struct)v, new HashMap<>()));
                } else {
                    // assume that JSON serialiser knows how to deal with it
                    outMap.put((String)k, v);
                }
            } else {
                throw new IllegalArgumentException("unsupported type in map key " + k.getClass());
            }
        }
        return outMap;
    }

    // get field value, recursing if necessary for struct types
    private Object getField(Type type, Object value) {

        switch (type) {
            // primitive types: just return value (JSON serialiser will deal with conversion later)
            case ARRAY:
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
                return convertMap((Map)value, new HashMap<>());
            case STRUCT:
                return convertStruct((Struct)value, new HashMap<>());
            default:
                throw new IllegalArgumentException("unknown type " + type);
        }
            
    }

    private String getHeaderForDocId(ConnectRecord<R> record) {
        Header value = record.headers().lastWithName("CLOUDANT_PREFERRED_ID");
        if (value != null && value.value() instanceof String) {
            return value.value().toString();
        }
        return null;
    }

}
