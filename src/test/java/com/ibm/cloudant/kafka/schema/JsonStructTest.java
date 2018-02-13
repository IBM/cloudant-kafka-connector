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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.FileReader;
import java.util.List;
import java.util.Map;

public class JsonStructTest {

    private static JsonObject schemaTypesObject;
    private static Schema schemaTypesExpected = SchemaBuilder.struct()
            .field("_id", Schema.OPTIONAL_STRING_SCHEMA)
            .field("_rev", Schema.OPTIONAL_STRING_SCHEMA)
            .field("string", Schema.OPTIONAL_STRING_SCHEMA)
            .field("double", Schema.OPTIONAL_FLOAT64_SCHEMA)
            .field("integer", Schema.OPTIONAL_FLOAT64_SCHEMA)
            .field("float", Schema.OPTIONAL_FLOAT64_SCHEMA)
            .field("long", Schema.OPTIONAL_FLOAT64_SCHEMA)
            .field("boolean", Schema.OPTIONAL_BOOLEAN_SCHEMA)
            .field("null", Schema.OPTIONAL_STRING_SCHEMA)
            .field("array_string", SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA).optional()
                    .build())
            .field("array_number", SchemaBuilder.array(Schema.OPTIONAL_FLOAT64_SCHEMA).optional()
                    .build())
            .field("array_boolean", SchemaBuilder.array(Schema.OPTIONAL_BOOLEAN_SCHEMA).optional
                    ().build())
            .field("object", SchemaBuilder.struct().field("nestedKey", Schema
                    .OPTIONAL_STRING_SCHEMA)
                    .optional()
                    .build())
            .optional()
            .build();

    @BeforeClass
    public static void setUp() throws Exception {
        schemaTypesObject = new Gson().fromJson(new FileReader("./src/test/resources/schema_types" +
                ".json"), JsonObject.class);
    }

    @Test
    public void testStringSchema() {
        Schema result = JsonStruct.jsonElementToSchema(new JsonPrimitive("test1"));
        assertEquals("Should get the correct schema type", Schema.Type.STRING, result.type());
    }

    @Test
    public void testNumberSchema() {
        Schema result = JsonStruct.jsonElementToSchema(new JsonPrimitive(1));
        assertEquals("Should get the correct schema type", Schema.Type.FLOAT64, result.type());
    }

    @Test
    public void testBooleanSchema() {
        Schema result = JsonStruct.jsonElementToSchema(new JsonPrimitive(false));
        assertEquals("Should get the correct schema type", Schema.Type.BOOLEAN, result.type());
    }

    @Test
    public void testNullSchema() {
        Schema result = JsonStruct.jsonElementToSchema(JsonNull.INSTANCE);
        assertEquals("Should get the correct schema type", Schema.Type.STRING, result.type());
        assertTrue("Should be optional", result.isOptional());
    }

    @Test
    public void testArraySchema() {
        JsonArray array = new JsonArray(2);
        array.add("a");
        array.add("b");
        Schema result = JsonStruct.jsonElementToSchema(array);
        Schema expectedSchema = SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA).optional()
                .build();
        assertEquals("Should get the correct schema type", Schema.Type.ARRAY, result.type());
        assertEquals("Should get the correct schema", expectedSchema, result);
    }

    @Test
    public void testObjectSchema() {
        JsonObject object = new JsonObject();
        object.addProperty("aString", "testString");
        object.addProperty("aNumber", 12);
        Schema result = JsonStruct.jsonElementToSchema(object);
        Schema expectedSchema = SchemaBuilder.struct()
                .field("aString", Schema.OPTIONAL_STRING_SCHEMA)
                .field("aNumber", Schema.OPTIONAL_FLOAT64_SCHEMA)
                .optional()
                .build();
        assertEquals("Should get the correct schema type", Schema.Type.STRUCT, result.type());
        assertEquals("Should get the correct schema", expectedSchema, result);
    }

    @Test
    public void testSchemaTypesObject() {
        Schema result = JsonStruct.jsonObjectToSchema(schemaTypesObject);
        assertEquals("Should get the correct schema type", Schema.Type.STRUCT, result.type());
        assertEquals("Should get the correct schema", schemaTypesExpected, result);
    }

    @Test
    public void testStructFromSchemaTypesObject() {
        Struct result = JsonStruct.jsonObjectToStruct(schemaTypesObject, JsonStruct
                .jsonObjectToSchema(schemaTypesObject));
        // Perform assertions on the resulting struct
        assertJsonObjectEqualsStruct(schemaTypesObject, result);
    }

    private void assertJsonObjectEqualsStruct(JsonObject object, Struct struct) {
        for (Map.Entry<String, JsonElement> entry : object.entrySet()) {
            String key = entry.getKey();
            JsonElement element = entry.getValue();
            assertJsonElementEqualsObject(element, struct.get(key));
        }
    }

    private void assertJsonArrayEqualsList(JsonArray array, List list) {
        assertEquals("There should be the same number of array elements", array.size(), list.size
                ());
        int index = 0;
        for (JsonElement e : array) {
            assertJsonElementEqualsObject(e, list.get((index)));
            index++;
        }
    }

    private void assertJsonElementEqualsObject(JsonElement element, Object value) {
        if (element.isJsonNull()) {
            assertEquals("The struct field should be null", null, value);
        } else if (element.isJsonObject()) {
            assertJsonObjectEqualsStruct(element.getAsJsonObject(), (Struct) value);
        } else if (element.isJsonArray()) {
            assertJsonArrayEqualsList(element.getAsJsonArray(), (List) value);
        } else if (element.isJsonPrimitive()) {
            JsonPrimitive primitive = element.getAsJsonPrimitive();
            if (primitive.isString()) {
                assertEquals("The struct string field should have the correct value", primitive
                        .getAsString(), value);
            } else if (primitive.isNumber()) {
                assertEquals("The struct number field should have the correct value", (Double)
                        primitive.getAsDouble(), value);
            } else if (primitive.isBoolean()) {
                assertEquals("The struct boolean field should have the correct value", primitive
                        .getAsBoolean(), value);
            } else {
                throw new IllegalArgumentException("Unknown JSON primitive");
            }
        } else {
            throw new IllegalArgumentException("Unknown JSON type");
        }
    }
}
