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
import com.google.gson.JsonObject;

import org.apache.kafka.connect.data.Struct;

public class DocumentAsSchemaStruct extends JsonObjectAsSchemaStruct {

    private final boolean flatten;

    /**
     * Converts a JSON document into a Schema.Type.STRUCT
     *
     * @param document the document to convert
     * @param flatten  whether to flatten nested objects and arrays
     * @return a Struct of the document
     */
    public static Struct convert(JsonObject document, boolean flatten) {
        return new DocumentAsSchemaStruct(document, flatten).convert();
    }

    private DocumentAsSchemaStruct(JsonObject object, boolean flatten) {
        // The document level struct should not be optional.
        super(object, false);
        this.flatten = flatten;
    }

    @Override
    protected void process(String key, JsonObject object) {
        if (flatten) {
            processFlattenedCollection(key, new JsonObjectAsFlattened(object).convert());
        } else {
            super.process(key, object);
        }
    }

    @Override
    protected void process(String key, JsonArray array) {
        if (flatten) {
            processFlattenedCollection(key, new JsonArrayAsFlattened(array).convert());
        } else {
            super.process(key, array);
        }
    }
}
