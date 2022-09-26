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
package com.ibm.cloud.cloudant.kafka.connect.transforms;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import java.io.FileReader;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.junit.Test;
import com.google.gson.Gson;
import com.ibm.cloud.cloudant.kafka.common.CloudantConst;
import com.ibm.cloud.cloudant.kafka.schema.DocumentHelpers;
import com.ibm.cloud.cloudant.kafka.schema.DocumentHelpers.Primitive;

public class MapToStructTest {

    private final MapToStruct transformer = new MapToStruct();

    private static void addPrimitivesToSchemaBuilder(SchemaBuilder builder) {
        EnumSet.allOf(Primitive.class).stream().forEach(e ->
            builder.field(e.name(), e.getSchema()
        ));
    }

    // Note Struct order is important (we use natural ordering of the field names)
    private static SchemaBuilder structWithPrimitives() {
        SchemaBuilder b = SchemaBuilder.struct().optional();
        addPrimitivesToSchemaBuilder(b);
        return b;
    }

    private static void addPrimitivesToStruct(Struct struct) {
        EnumSet.allOf(Primitive.class).forEach(e ->
            struct.put(e.name(), e.getValue())
        );
    }

    private static Struct makeStructWithPrimitives() {
        Schema s = structWithPrimitives().build();
        Struct struct = new Struct(s);
        addPrimitivesToStruct(struct);
        return struct;
    }

    private SourceRecord wrapInRecord(Map<String, Object> doc) {
        return new SourceRecord(Collections.emptyMap(), Collections.emptyMap(), "test", null, null, null, doc);
    }

    private SchemaAndValue transformAndGetSchemaAndValue(SourceRecord original) {
        SourceRecord transformed = transformer.apply(original);
        return new SchemaAndValue(transformed.valueSchema(), transformed.value());
    }

    private SchemaAndValue transformMap(Map<String, Object> original) {
        return transformAndGetSchemaAndValue(wrapInRecord(original));
    }

    private void assertStruct(Struct expected, SchemaAndValue actual) {
        Struct structValue = (Struct) actual.value();
        // Assert the field list first because it is more helpful for debug
        assertEquals("The schema fields should match the expected", expected.schema().fields(), actual.schema().fields());
        // Then assert schema which covers other things like optional
        assertEquals("The record schema should match the expected", expected.schema(), actual.schema());
        assertEquals("The record value struct schema should match the expected", expected.schema(), structValue.schema());
        assertEquals("The structs should be equal", expected, structValue);
    }

    @Test
    public void testMapOfPrimitives() {
        Struct expected = makeStructWithPrimitives();
        SchemaAndValue actual = transformMap(DocumentHelpers.primitiveTypesAsMap());
        assertStruct(expected, actual);
    }

    @Test
    public void testArray() {
        Schema expectedSchema = SchemaBuilder.struct().optional().field("array", SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA).optional().build()).build();
        List<Object> array = DocumentHelpers.arrayOf(() -> Primitive.STRING.getValue(), 3);
        Map<String, Object> original = Collections.singletonMap("array", array);
        Struct expected = new Struct(expectedSchema).put("array", array);
        SchemaAndValue actual = transformMap(original);
        assertStruct(expected, actual);
    }

    @Test(expected = DataException.class)
    public void testMixedArray() {
        transformMap(Collections.singletonMap("array", DocumentHelpers.allTypesAsArray()));
    }

    @Test
    public void testEmptyArray() {
        Schema expectedSchema = SchemaBuilder.struct().optional().field("array", SchemaBuilder.array(MapToStruct.NULL_VALUE_SCHEMA).optional().build()).build();
        List<Object> array = Collections.emptyList();
        Map<String, Object> original = Collections.singletonMap("array", array);
        Struct expected = new Struct(expectedSchema).put("array", array);
        SchemaAndValue actual = transformMap(original);
        assertStruct(expected, actual);
    }

    @Test
    public void testArrayWithOnlyNull() {
        Schema expectedSchema = SchemaBuilder.struct().optional().field("array", SchemaBuilder.array(MapToStruct.NULL_VALUE_SCHEMA).optional().build()).build();
        List<Object> array = Collections.singletonList(null);
        Map<String, Object> original = Collections.singletonMap("array", array);
        Struct expected = new Struct(expectedSchema).put("array", array);
        SchemaAndValue actual = transformMap(original);
        assertStruct(expected, actual);
    }

    @Test
    public void testArrayOfMap() {
        final Struct nestedMap = makeStructWithPrimitives();
        Schema expectedSchema = SchemaBuilder.struct().optional().field("array", SchemaBuilder.array(nestedMap.schema()).optional().build()).build();
        Struct expected = new Struct(expectedSchema).put("array", DocumentHelpers.arrayOf(() -> nestedMap, 7));
        Map<String, Object> original = Collections.singletonMap("array", DocumentHelpers.arrayOf(() -> DocumentHelpers.primitiveTypesAsMap(), 7));
        SchemaAndValue actual = transformMap(original);
        assertStruct(expected, actual);
    }

    @Test
    public void testArrayOfArray() {
        List<Object> arrayOfArrays = DocumentHelpers.arrayOf(() -> DocumentHelpers.arrayOf(() -> Primitive.BIG_DECIMAL.getValue(), 2), 2);
        Map<String, Object> original = Collections.singletonMap("array", arrayOfArrays);

        Schema nestedArraySchema = SchemaBuilder.array(Primitive.BIG_DECIMAL.getSchema()).optional().build();
        Schema arraySchema = SchemaBuilder.array(nestedArraySchema).optional().build();
        Schema expectedSchema = SchemaBuilder.struct().optional().field("array", arraySchema).build();
        Struct expected = new Struct(expectedSchema).put("array", arrayOfArrays);
        SchemaAndValue actual = transformMap(original);
        assertStruct(expected, actual);
    }

    @Test(expected = DataException.class)
    public void testArrayOfMixedArray() {
        List<Object> arrayOfArrays = DocumentHelpers.arrayOf(() -> DocumentHelpers.allTypesAsArray(), 5);
        Map<String, Object> original = Collections.singletonMap("array", arrayOfArrays);
        transformMap(original);
    }

    @Test
    public void testMapOfMap() {
        Map<String, Object> original = Collections.singletonMap("foo", Collections.singletonMap("bar", "baz"));
        Schema nestedExpectedSchema = SchemaBuilder.struct().optional().field("bar", Schema.OPTIONAL_STRING_SCHEMA).build();
        Schema expectedSchema = SchemaBuilder.struct().optional().field("foo", nestedExpectedSchema).build();
        Struct expected = new Struct(expectedSchema).put("foo", new Struct(nestedExpectedSchema).put("bar", "baz"));
        SchemaAndValue actual = transformMap(original);
        assertStruct(expected, actual);
    }

    @Test
    public void testMapOfNullValue() {
        Map<String, Object> original = Collections.singletonMap("optionalFoo", null);
        Struct expected = new Struct(SchemaBuilder.struct().optional().field("optionalFoo", MapToStruct.NULL_VALUE_SCHEMA).build()).put("optionalFoo", null);
        SchemaAndValue actual = transformMap(original);
        assertStruct(expected, actual);
    }

    @Test
    public void testMapOfAllTypes() {
        List<Object> nestedArray = DocumentHelpers.arrayOf(() -> 1, 17);
        Map<String, Object> original = DocumentHelpers.allTypesAsMap(() -> DocumentHelpers.primitiveTypesAsMap(), () -> nestedArray);
        original.put(CloudantConst.CLOUDANT_DOC_ID, "mytestfoodoc");
        original.put(CloudantConst.CLOUDANT_REV, "42-0123456789abcdef0123456789abcdef");
        Struct nestedMap = makeStructWithPrimitives();
        SchemaBuilder expectedBuilder = SchemaBuilder.struct().optional();
        // Add the primitives
        addPrimitivesToSchemaBuilder(expectedBuilder);
        // Add an ID and rev
        expectedBuilder.field(CloudantConst.CLOUDANT_DOC_ID, Schema.OPTIONAL_STRING_SCHEMA);
        expectedBuilder.field(CloudantConst.CLOUDANT_REV, Schema.OPTIONAL_STRING_SCHEMA);
        // Add the nested map and array
        expectedBuilder.field("array", SchemaBuilder.array(Schema.OPTIONAL_INT32_SCHEMA).optional().build());
        expectedBuilder.field("map", nestedMap.schema());
        Schema expectedSchema = expectedBuilder.build();
        Struct expected = new Struct(expectedSchema);
        expected.put(CloudantConst.CLOUDANT_DOC_ID, "mytestfoodoc");
        expected.put(CloudantConst.CLOUDANT_REV, "42-0123456789abcdef0123456789abcdef");
        addPrimitivesToStruct(expected);
        expected.put("array", nestedArray);
        expected.put("map", nestedMap);
        SchemaAndValue actual = transformMap(original);
        assertStruct(expected, actual);
    }

    @Test
    public void testTypesDoc() throws Exception {
        try(FileReader reader = new FileReader("./src/test/resources/schema_types.json")) {
            Map<String, Object> sampleDoc = new Gson().fromJson(reader, Map.class);
            SchemaAndValue actual = transformMap(sampleDoc);
            // We don't assert the whole schema here, just validate we got one and no exceptions were thrown
            assertNotNull("The schema should not be null", actual.schema());
            assertNotNull("The value should not be null", actual.value());
            Struct struct = (Struct) actual.value();
            sampleDoc.entrySet().stream().filter(e -> {return !"z_object".equals(e.getKey());}).forEach(e -> {
                assertEquals(e.getValue(), struct.get(e.getKey()));
            });
            assertEquals(
                ((Map<String, Object>) sampleDoc.get("z_object")).get("nestedKey"),
                ((Struct) struct.get("z_object")).get("nestedKey")
            );
        }
    }

    @Test
    public void testSampleDoc() throws Exception {
        try(FileReader reader = new FileReader("./src/test/resources/sample_doc_from_data.json")) {
            Map<String, Object> sampleDoc = new Gson().fromJson(reader, Map.class);
            SourceRecord sourceRecord = wrapInRecord(sampleDoc);
            // This sample doc has mixed type arrays, we need to flatten first
            try (ArrayFlatten flattener = new ArrayFlatten()) {
                SourceRecord flattenedDoc = flattener.apply(sourceRecord);
                SchemaAndValue actual = transformAndGetSchemaAndValue(flattenedDoc);
                // We don't assert the whole schema here, just validate we got one and no exceptions were thrown
                assertNotNull("The schema should not be null", actual.schema());
                assertNotNull("The value should not be null", actual.value());
            }
        }
    }

}
