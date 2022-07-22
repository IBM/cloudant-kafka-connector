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

import static org.apache.kafka.connect.data.Schema.OPTIONAL_BOOLEAN_SCHEMA;
import static org.apache.kafka.connect.data.Schema.OPTIONAL_FLOAT64_SCHEMA;
import static org.apache.kafka.connect.data.Schema.OPTIONAL_STRING_SCHEMA;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.ibm.cloud.cloudant.kafka.common.MessageKey;
import com.ibm.cloud.cloudant.kafka.common.utils.ResourceBundleUtil;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * An abstract super class for converting JSON collections (i.e. Object or Array) to some other
 * type.
 *
 * @param <T> the type to convert to
 */
public abstract class JsonCollectionConverter<T> {

    // Use a map type that maintains the insertion order, this is important for array processing
    // For objects we sort at the generation phase
    protected final Map<String, SchemaValue> fields = new LinkedHashMap<>();

    /**
     * To be overridden by subclasses to process the JSON collection and convert.
     *
     * @return an instance of the required type
     */
    protected abstract T convert();

    /**
     * To be overridden by subclasses and called by convert to get the specific type required.
     *
     * @return an instance of the required type
     */
    protected abstract T generate();

    /**
     * To be overridden by subclasses with specifics of how to process a JSON object.
     *
     * @param key    the key referencing the object
     * @param object the JSON object to process
     */
    protected abstract void process(String key, JsonObject object);

    /**
     * To be overridden by subclasses with specifics of how to process a JSON array.
     *
     * @param key   the key referencing the array
     * @param array the JSON array to process
     */
    protected abstract void process(String key, JsonArray array);

    /**
     * Process a JSON element from the collection
     *
     * @param key     the key referencing the element (an index in the case of an array)
     * @param element the element to process
     */
    protected void process(String key, JsonElement element) {
        if (element.isJsonNull()) {
            fields.put(key, SchemaValue.NULL);
        } else if (element.isJsonPrimitive()) {
            process(key, element.getAsJsonPrimitive());
        } else if (element.isJsonArray()) {
            process(key, element.getAsJsonArray());
        } else if (element.isJsonObject()) {
            process(key, element.getAsJsonObject());
        } else {
            throw new IllegalArgumentException(ResourceBundleUtil.get(MessageKey
                    .CLOUDANT_STRUCT_SCHEMA_JSON_ELEMENT));
        }
    }

    /**
     * Process a JSON primitive from the collection
     *
     * @param key       the key referencing the primitive
     * @param primitive the primitive to process
     */
    protected void process(String key, JsonPrimitive primitive) {
        if (primitive.isBoolean()) {
            fields.put(key, new SchemaValue(OPTIONAL_BOOLEAN_SCHEMA, primitive
                    .getAsBoolean()));
        } else if (primitive.isNumber()) {
            fields.put(key, new SchemaValue(OPTIONAL_FLOAT64_SCHEMA, primitive.getAsDouble()));
        } else if (primitive.isString()) {
            fields.put(key, new SchemaValue(OPTIONAL_STRING_SCHEMA, primitive.getAsString
                    ()));
        } else {
            throw new IllegalArgumentException(ResourceBundleUtil.get(MessageKey
                    .CLOUDANT_STRUCT_SCHEMA_JSON_PRIMITIVE));
        }
    }

    /**
     * Class to encapsulate a schema and value pair from an element to field conversion.
     */
    protected static final class SchemaValue {

        static SchemaValue NULL = new SchemaValue(OPTIONAL_STRING_SCHEMA, null);

        final Schema schema;
        final Object value;

        SchemaValue(Schema schema, Object value) {
            this.schema = schema;
            this.value = value;
        }
    }

    /**
     * Process a JSON array into Schema.Type.ARRAY
     *
     * @param key   the key referencing the array
     * @param array the JSON array
     */
    void processArrayAsSchemaArray(String key, JsonArray array) {
        SchemaValue arraySchemaValue = new JsonArrayAsSchemaArray(array).convert();
        fields.put(key, arraySchemaValue);
    }

    /**
     * Process a JSON array into Schema.Type.STRUCT
     *
     * @param key    the key referencing the object
     * @param object the JSON object
     */
    void processObjectAsSchemaStruct(String key, JsonObject object) {
        Struct struct = new JsonObjectAsSchemaStruct(object).convert();
        fields.put(key, new SchemaValue(struct.schema(), struct));
    }

    /**
     * Process a JSON array into flattened fields, named with the index of the element in the JSON
     * array appended.
     *
     * @param key   the key referencing the array
     * @param array the JSON array to process
     */
    void processArrayAsFlattened(String key, JsonArray array) {
        processFlattenedCollection(key, new JsonArrayAsFlattened(array).convert());
    }

    /**
     * Process a JSON object into flattened fields, named with the nested property appended.
     *
     * @param key    the key referencing the object
     * @param object the JSON object to process
     */
    void processObjectAsFlattened(String key, JsonObject object) {
        processFlattenedCollection(key, new JsonObjectAsFlattened(object).convert());
    }

    /**
     * Process a map of field name to schema values (i.e. an arbitrary JSON collection) into this
     * JSON collection. Modifies the key names with the appropriate appendixes.
     *
     * @param key       the key referencing the JSON collection to merge
     * @param flattened map of field name to schema value
     */
    void processFlattenedCollection(String key, Map<String, SchemaValue> flattened) {
        flattened.forEach((k, sv) -> fields.put(key + "." + k, sv));
    }
}
