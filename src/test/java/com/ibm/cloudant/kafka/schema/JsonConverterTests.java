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
import static org.junit.Assert.assertNotNull;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.ibm.cloudant.kafka.common.MessageKey;
import com.ibm.cloudant.kafka.common.utils.ResourceBundleUtil;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class JsonConverterTests {

    private static JsonObject schemaTypesObject;
    private static Schema schemaTypesExpected = addNested(structWithPrimitives())
            .optional()
            .build();
    private static Schema schemaTypesExpectedNotOptional = addNested(structWithPrimitives())
            .build();
    private static Schema schemaTypesFlattenedExpected = addFlattened(structWithPrimitives())
            .build();

    // Note Struct order is important (we use natural ordering of the field names)
    private static SchemaBuilder structWithPrimitives() {
        return SchemaBuilder.struct()
                .field("_id", Schema.OPTIONAL_STRING_SCHEMA)
                .field("_rev", Schema.OPTIONAL_STRING_SCHEMA)
                .field("boolean", Schema.OPTIONAL_BOOLEAN_SCHEMA)
                .field("double", Schema.OPTIONAL_FLOAT64_SCHEMA)
                .field("float", Schema.OPTIONAL_FLOAT64_SCHEMA)
                .field("integer", Schema.OPTIONAL_FLOAT64_SCHEMA)
                .field("long", Schema.OPTIONAL_FLOAT64_SCHEMA)
                .field("null", Schema.OPTIONAL_STRING_SCHEMA)
                .field("string", Schema.OPTIONAL_STRING_SCHEMA);
    }

    private static SchemaBuilder addNested(SchemaBuilder builder) {
        return builder
                .field("z_array_boolean", SchemaBuilder.array(Schema.OPTIONAL_BOOLEAN_SCHEMA)
                        .optional
                                ().build())
                .field("z_array_number", SchemaBuilder.array(Schema.OPTIONAL_FLOAT64_SCHEMA)
                        .optional()
                        .build())
                .field("z_array_string", SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA)
                        .optional()
                        .build())
                .field("z_object", SchemaBuilder.struct().field("nestedKey", Schema
                        .OPTIONAL_STRING_SCHEMA)
                        .optional()
                        .build());
    }

    private static SchemaBuilder addFlattened(SchemaBuilder builder) {
        return builder
                .field("z_array_boolean.0", Schema.OPTIONAL_BOOLEAN_SCHEMA)
                .field("z_array_boolean.1", Schema.OPTIONAL_BOOLEAN_SCHEMA)
                .field("z_array_number.0", Schema.OPTIONAL_FLOAT64_SCHEMA)
                .field("z_array_number.1", Schema.OPTIONAL_FLOAT64_SCHEMA)
                .field("z_array_number.2", Schema.OPTIONAL_FLOAT64_SCHEMA)
                .field("z_array_number.3", Schema.OPTIONAL_FLOAT64_SCHEMA)
                .field("z_array_string.0", Schema.OPTIONAL_STRING_SCHEMA)
                .field("z_array_string.1", Schema.OPTIONAL_STRING_SCHEMA)
                .field("z_array_string.2", Schema.OPTIONAL_STRING_SCHEMA)
                .field("z_array_string.3", Schema.OPTIONAL_STRING_SCHEMA)
                .field("z_object.nestedKey", Schema.OPTIONAL_STRING_SCHEMA);
    }

    @BeforeClass
    public static void setUp() throws Exception {
        schemaTypesObject = new Gson().fromJson(new FileReader("./src/test/resources/schema_types" +
                ".json"), JsonObject.class);
    }

    private void testJsonElementToSchemaValue(JsonElement element, Schema expected, Object
            expectedValue) {
        String testKey = "test";
        JsonCollectionConverter c = new JsonObjectAsSchemaStruct(new JsonObject());
        c.process(testKey, element);
        JsonCollectionConverter.SchemaValue sv = (JsonCollectionConverter.SchemaValue) c.fields
                .get(testKey);
        assertSingleSchemaValue(sv, expected, expectedValue);
    }

    private void assertSingleSchemaValue(JsonCollectionConverter.SchemaValue sv, Schema expected,
                                         Object expectedValue) {
        assertNotNull("There should be an entry in the fields map", sv);
        assertEquals("The schema should be correct", expected, sv.schema);
        assertEquals("The value should be correct", expectedValue, sv.value);
    }

    @Test
    public void testString() {
        String t = "test1";
        testJsonElementToSchemaValue(new JsonPrimitive(t), Schema.OPTIONAL_STRING_SCHEMA, t);
    }

    @Test
    public void testNumbers() {
        Integer i = 4;
        Long l = 17l;
        Float f = 0.6f;
        Double d = 3.14d;
        testJsonElementToSchemaValue(new JsonPrimitive(i), Schema.OPTIONAL_FLOAT64_SCHEMA, i
                .doubleValue());
        testJsonElementToSchemaValue(new JsonPrimitive(l), Schema.OPTIONAL_FLOAT64_SCHEMA, l
                .doubleValue());
        testJsonElementToSchemaValue(new JsonPrimitive(f), Schema.OPTIONAL_FLOAT64_SCHEMA, f
                .doubleValue());
        testJsonElementToSchemaValue(new JsonPrimitive(d), Schema.OPTIONAL_FLOAT64_SCHEMA, d
                .doubleValue());
    }

    @Test
    public void testBoolean() {
        testJsonElementToSchemaValue(new JsonPrimitive(false), Schema.OPTIONAL_BOOLEAN_SCHEMA,
                false);
    }

    @Test
    public void testNull() {
        testJsonElementToSchemaValue(JsonNull.INSTANCE, Schema.OPTIONAL_STRING_SCHEMA, null);
    }

    @Test
    public void testArray() {
        JsonArray array = new JsonArray(2);
        array.add("a");
        array.add("b");
        Schema expectedSchema = SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA).optional()
                .build();
        testJsonElementToSchemaValue(array, expectedSchema, Arrays.asList("a", "b"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMixedArray() {
        JsonArray array = new JsonArray(2);
        array.add("a");
        array.add(1);
        JsonArrayAsSchemaArray c = new JsonArrayAsSchemaArray(array);
        try {
            c.convert();
        } catch (IllegalArgumentException e) {
            assertEquals("The exception message should be for mixed arrays",
                    ResourceBundleUtil.get(MessageKey.CLOUDANT_STRUCT_SCHEMA_JSON_MIXED_ARRAY), e
                            .getMessage());
            throw e;
        }
    }

    @Test
    public void testMixedArrayObject() {
        JsonObject a = new JsonObject();
        a.addProperty("string", "a");
        JsonObject b = new JsonObject();
        b.addProperty("number", 2);
        JsonArray array = new JsonArray(2);
        array.add(a);
        array.add(b);
        Schema expectedMergedStructSchema = SchemaBuilder.struct().optional().field("number",
                Schema.OPTIONAL_FLOAT64_SCHEMA).field("string", Schema.OPTIONAL_STRING_SCHEMA)
                .build();
        Schema expectedSchema = SchemaBuilder.array(expectedMergedStructSchema).optional().build();
        List<Struct> expectedStructs = new ArrayList<>();
        expectedStructs.add(new Struct(expectedMergedStructSchema).put("string", "a"));
        expectedStructs.add(new Struct(expectedMergedStructSchema).put("number", 2d));
        testJsonElementToSchemaValue(array, expectedSchema, expectedStructs);
    }


    @Test
    public void testObject() {
        JsonObject object = new JsonObject();
        object.addProperty("aString", "testString");
        object.addProperty("aNumber", 12);
        Schema expectedSchema = SchemaBuilder.struct()
                .field("aNumber", Schema.OPTIONAL_FLOAT64_SCHEMA)
                .field("aString", Schema.OPTIONAL_STRING_SCHEMA)
                .optional()
                .build();
        Struct expected = new Struct(expectedSchema);
        expected.put("aString", "testString");
        expected.put("aNumber", 12d);
        testJsonElementToSchemaValue(object, expectedSchema, expected);
    }

    @Test
    public void testFlattenedArray() {
        JsonArray array = new JsonArray(2);
        array.add("a");
        array.add("b");
        Map<String, JsonCollectionConverter.SchemaValue> flat = new JsonArrayAsFlattened(array)
                .convert();
        assertSingleSchemaValue(flat.get("0"), Schema.OPTIONAL_STRING_SCHEMA, "a");
        assertSingleSchemaValue(flat.get("1"), Schema.OPTIONAL_STRING_SCHEMA, "b");
    }

    @Test
    public void testFlattenedObject() {
        JsonObject object = new JsonObject();
        object.addProperty("aString", "testString");
        object.addProperty("aNumber", 12);
        Map<String, JsonCollectionConverter.SchemaValue> flat = new JsonObjectAsFlattened(object)
                .convert();
        assertSingleSchemaValue(flat.get("aString"), Schema.OPTIONAL_STRING_SCHEMA, "testString");
        assertSingleSchemaValue(flat.get("aNumber"), Schema.OPTIONAL_FLOAT64_SCHEMA, 12d);
    }

    @Test
    public void testFlattenedArrayMixed() {
        JsonArray array = new JsonArray(2);
        array.add("a");
        array.add(1);
        JsonArrayAsFlattened f = new JsonArrayAsFlattened(array);
        Map<String, JsonCollectionConverter.SchemaValue> elements = f.convert();
        assertSingleSchemaValue(elements.get("0"), Schema.OPTIONAL_STRING_SCHEMA, "a");
        assertSingleSchemaValue(elements.get("1"), Schema.OPTIONAL_FLOAT64_SCHEMA, 1d);
    }

    @Test
    public void testFlattenedArrayMixedObject() {
        JsonObject a = new JsonObject();
        a.addProperty("string", "a");
        JsonObject b = new JsonObject();
        b.addProperty("number", 2);
        JsonArray array = new JsonArray(2);
        array.add(a);
        array.add(b);
        JsonArrayAsFlattened f = new JsonArrayAsFlattened(array);
        Map<String, JsonCollectionConverter.SchemaValue> elements = f.convert();
        assertSingleSchemaValue(elements.get("0.string"), Schema.OPTIONAL_STRING_SCHEMA, "a");
        assertSingleSchemaValue(elements.get("1.number"), Schema.OPTIONAL_FLOAT64_SCHEMA, 2d);
    }

    @Test
    public void testSchemaTypesObject() {
        Struct s = new JsonObjectAsSchemaStruct(schemaTypesObject).convert();
        Schema result = s.schema();
        assertEquals("Should get the correct schema type", Schema.Type.STRUCT, result.type());
        assertEquals("Should get the correct schema", schemaTypesExpected, result);
        assertJsonObjectEqualsStruct(schemaTypesObject, s);
    }

    @Test
    public void testSchemaTypesDocument() {
        Struct s = DocumentAsSchemaStruct.convert(schemaTypesObject, false);
        assertEquals("Should get the correct schema type", Schema.Type.STRUCT, s.schema().type());
        assertEquals("Should get the correct schema", schemaTypesExpectedNotOptional, s.schema());
        assertJsonObjectEqualsStruct(schemaTypesObject, s);
    }

    @Test
    public void testSchemaTypesFlattenedDocument() {
        Struct s = DocumentAsSchemaStruct.convert(schemaTypesObject, true);
        Schema result = s.schema();
        assertEquals("Should get the correct schema type", Schema.Type.STRUCT, result.type());
        assertEquals("Should get the correct schema", schemaTypesFlattenedExpected, result);
        // Create the flattened object to assert against
        JsonObject expectedFlattened = new JsonObject();
        // Copy all the non-flattened props
        for (Map.Entry<String, JsonElement> p : schemaTypesObject.entrySet()) {
            String k = p.getKey();
            JsonElement e = p.getValue();
            if (!e.isJsonObject() && !e.isJsonArray()) {
                expectedFlattened.add(k, e);
            }
        }
        // Add the rest
        int i = 0;
        for (String a : new String[]{"Aberystwyth", "Bangor", "Cardiff", "Dorchester"}) {
            expectedFlattened.add("z_array_string." + i, new JsonPrimitive(a));
            i++;
        }
        i = 0;
        for (double d : new double[]{0.8394783663178513d, -192741966d, 0.032276213d,
                -2044357298457380400d}) {
            expectedFlattened.add("z_array_number." + i, new JsonPrimitive(d));
            i++;
        }
        i = 0;
        for (boolean b : new boolean[]{false, true}) {
            expectedFlattened.add("z_array_boolean." + i, new JsonPrimitive(b));
            i++;
        }
        expectedFlattened.add("z_object.nestedKey", new JsonPrimitive("㸪\uE69A\uD839\uDC5B" +
                "钽麳볬ऻ\uF216⨝쪔］猵ઘᷛ鱽"));
        assertJsonObjectEqualsStruct(expectedFlattened, s);
    }

    @Test
    public void testSampleDoc() throws Exception {
        JsonObject sampleDoc = new Gson().fromJson(new FileReader("" +
                "./src/test/resources/sample_doc_from_data.json"), JsonObject.class);
        Struct sampleStruct = DocumentAsSchemaStruct.convert(sampleDoc, false);
        // We don't assert the whole schema here, just validate we got one and no exceptions were
        // thrown
        assertNotNull(sampleStruct);
        assertEquals("Should be a struct type schema", Schema.Type.STRUCT, sampleStruct.schema()
                .type());
        assertEquals("Should not be optional", false, sampleStruct.schema()
                .isOptional());
        // _id, _rev, doc, id, value, key
        assertEquals("Should have the correct number of fields", 6, sampleStruct.schema().fields
                ().size());
    }

    @Test
    public void testSampleDocFlattened() throws Exception {
        JsonObject sampleDoc = new Gson().fromJson(new FileReader("" +
                "./src/test/resources/sample_doc_from_data.json"), JsonObject.class);
        Struct sampleStruct = DocumentAsSchemaStruct.convert(sampleDoc, true);
        // We don't assert the whole schema here, just validate we got one and no exceptions were
        // thrown
        assertNotNull(sampleStruct);
        assertEquals("Should be a struct type schema", Schema.Type.STRUCT, sampleStruct.schema()
                .type());
        assertEquals("Should not be optional", false, sampleStruct.schema()
                .isOptional());
        //
        assertEquals("Should have the correct number of fields", 314, sampleStruct.schema().fields
                ().size());
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
