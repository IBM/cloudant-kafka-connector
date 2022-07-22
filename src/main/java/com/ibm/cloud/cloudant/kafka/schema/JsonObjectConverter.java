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

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import java.util.Map;
import java.util.Set;

/**
 * Class for general JsonObject converters to extend.
 *
 * @param <T> the type to generate from the object
 */
public abstract class JsonObjectConverter<T> extends JsonCollectionConverter<T> {

    private final Set<Map.Entry<String, JsonElement>> properties;

    protected JsonObjectConverter(JsonObject object) {
        this.properties = object.entrySet();
    }

    /**
     * Traverses the properties of a JsonObject calling
     * {@link JsonCollectionConverter#process(String, JsonElement)} in turn for each.
     * Finally calls generate() to produce the required type.
     *
     * @return the type T from a call to generate()
     */
    @Override
    protected T convert() {
        for (Map.Entry<String, JsonElement> property : properties) {
            String key = property.getKey();
            JsonElement value = property.getValue();
            process(key, value);
        }
        return generate();
    }

}
