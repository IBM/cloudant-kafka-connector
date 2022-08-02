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

import java.util.Map;

/**
 * A class for processing a JsonArray into a flattened Map of fields.
 */
public class JsonArrayAsFlattened extends JsonArrayConverter<Map<String, JsonCollectionConverter
        .SchemaValue>> {

    protected JsonArrayAsFlattened(JsonArray array) {
        super(array);
    }

    @Override
    protected Map<String, JsonCollectionConverter.SchemaValue> generate() {
        return this.fields;
    }

    @Override
    protected void process(String key, JsonObject object) {
        processObjectAsFlattened(key, object);
    }

    @Override
    protected void process(String key, JsonArray array) {
        processArrayAsFlattened(key, array);
    }
}
