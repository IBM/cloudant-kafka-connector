/*
 * Copyright © 2018 IBM Corp. All rights reserved.
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
package com.ibm.cloud.cloudant.kafka.schema;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import java.util.Comparator;
import java.util.Map;

/**
 * A class for converting a JsonObject into a Schema.Type.STRUCT
 */
public class JsonObjectAsSchemaStruct extends JsonObjectConverter<Struct> {

    private final SchemaBuilder sb = SchemaBuilder.struct();

    JsonObjectAsSchemaStruct(JsonObject object) {
        this(object, true);
    }

    /**
     * @param object   the JsonObject to convert
     * @param optional whether the resulting Struct should be optional
     */
    JsonObjectAsSchemaStruct(JsonObject object, boolean optional) {
        super(object);
        if (optional) {
            sb.optional();
        }
    }

    @Override
    protected Struct generate() {
        // The order is important in a org.apache.kafka.connect.data.Schema.Type.STRUCT
        // We can't guarantee that the JSON encounter order will be the same because the JSON spec
        // https://tools.ietf.org/html/rfc7159#section-1 states that
        // "An object is an unordered collection"
        // As such we sort by the natural order of keys for consistency in building Struct schemas.
        fields.entrySet().stream().sorted(Comparator.comparing(Map.Entry::getKey)).forEach(f ->
                sb.field(f.getKey(), f.getValue().schema));
        Struct struct = new Struct(sb.build());
        fields.forEach((k, sv) -> struct.put(k, sv.value));
        return struct;
    }

    @Override
    protected void process(String key, JsonObject object) {
        processObjectAsSchemaStruct(key, object);
    }

    @Override
    protected void process(String key, JsonArray array) {
        processArrayAsSchemaArray(key, array);
    }
}
