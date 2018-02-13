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
package com.ibm.cloudant.kafka.schema;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class JsonStruct {

    public static Schema jsonObjectToSchema(JsonObject o) {
        SchemaBuilder sb = SchemaBuilder.struct();
        for (Map.Entry<String, JsonElement> field : o.entrySet()) {
            sb.field(field.getKey(), jsonElementToSchema(field.getValue()));
        }
        sb.optional();
        return sb.build();
    }

    public static Struct jsonObjectToStruct(JsonObject o, Schema schema) {
        Struct struct = new Struct(schema);
        for (Field f : schema.fields()) {
            struct.put(f, fromJsonElement(o.get(f.name()), f.schema()));
        }
        return struct;
    }

    private static Object fromJsonElement(JsonElement e, Schema s) {
        switch (s.type()) {
            case STRUCT:
                return jsonObjectToStruct(e.getAsJsonObject(), s);
            case ARRAY:
                JsonArray jsonArray = e.getAsJsonArray();
                List array = new ArrayList(jsonArray.size());
                for (JsonElement arrayElement : jsonArray) {
                    array.add(fromJsonElement(arrayElement, s.valueSchema()));
                }
                return array;
            case STRING:
                // We always map JsonNull to an optional string, so we need to check again if it
                // is a JSON null before we try to get it
                if (e.isJsonNull()) {
                    return null;
                } else {
                    return e.getAsString();
                }
            case BOOLEAN:
                return e.getAsBoolean();
            case FLOAT64:
                return e.getAsDouble();
            default:
                return null;
        }
    }

    private static Schema jsonArrayToSchema(JsonArray a) {
        if (a.size() > 0) {
            // Can't do mixed type arrays in Kafka
            // Base the array type on the first element
            return SchemaBuilder.array(jsonElementToSchema(a.get(0))).optional().build();
        } else {
            // Treat an empty array as a String type
            return SchemaBuilder.array(SchemaBuilder.OPTIONAL_STRING_SCHEMA).optional().build();
        }
    }

    private static Schema jsonPrimitiveToSchema(JsonPrimitive p) {
        if (p.isBoolean()) {
            return Schema.OPTIONAL_BOOLEAN_SCHEMA;
        } else if (p.isNumber()) {
            return Schema.OPTIONAL_FLOAT64_SCHEMA;
        } else {
            // String
            return Schema.OPTIONAL_STRING_SCHEMA;
        }
    }

    static Schema jsonElementToSchema(JsonElement e) {
        if (e.isJsonNull()) {
            return Schema.OPTIONAL_STRING_SCHEMA;
        } else if (e.isJsonPrimitive()) {
            return jsonPrimitiveToSchema(e.getAsJsonPrimitive());
        } else if (e.isJsonArray()) {
            return jsonArrayToSchema(e.getAsJsonArray());
        } else {
            // Object
            return jsonObjectToSchema(e.getAsJsonObject());
        }
    }

}
