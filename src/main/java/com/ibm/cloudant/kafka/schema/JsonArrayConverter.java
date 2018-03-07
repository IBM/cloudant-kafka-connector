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

/**
 * Class for general JsonArray converters to extend.
 *
 * @param <T> the type to generate from the array
 */
public abstract class JsonArrayConverter<T> extends JsonCollectionConverter<T> {

    private final JsonArray array;

    protected JsonArrayConverter(JsonArray array) {
        this.array = array;
    }

    /**
     * Traverses the elements of a JsonArray calling
     * {@link JsonCollectionConverter#process(String, JsonElement)} in turn for each. Increments the
     * element indexes and then calls generate() to produce the required type.
     *
     * @return the type T from a call to generate()
     */
    @Override
    protected T convert() {
        int index = 0;
        for (JsonElement element : array) {
            process(String.valueOf(index), element);
            index++;
        }
        return generate();
    }

}
