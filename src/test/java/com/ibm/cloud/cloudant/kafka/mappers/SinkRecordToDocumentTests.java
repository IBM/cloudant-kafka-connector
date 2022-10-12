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
package com.ibm.cloud.cloudant.kafka.mappers;

import com.ibm.cloud.cloudant.v1.model.Document;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.ibm.cloud.cloudant.kafka.mappers.SinkRecordToDocument.HEADER_DOC_ID_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

public class SinkRecordToDocumentTests {

    // for these tests we will operate on SinkRecords, but they could just as easily be SourceRecords - for our purposes they are equivalent
    SinkRecordToDocument mapper = new SinkRecordToDocument();

    @Test
    public void testConvertToMapSchema() {
        // given...
        Schema s = SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA);
        Map<String, String> value = new HashMap<>();
        value.put("hello", "world");
        SinkRecord sr = new SinkRecord("test", 13, null, "0001", s, value, 0);
        // when...
        Document converted = mapper.apply(sr);
        // then...
        assertEquals("world", converted.get("hello"));
    }

    @Test
    public void testConvertToMapNoSchema() {
        // given...
        Schema s = null; // no schema
        Map<String, String> value = new HashMap<>();
        value.put("hello", "world");
        SinkRecord sr = new SinkRecord("test", 13, null, "0001", s, value, 0);
        // when...
        Document converted = mapper.apply(sr);
        // then...
        assertEquals("world", converted.get("hello"));
    }

    @Test
    public void testConvertToMapNoSchemaWithHeader() {
        // given...
        Schema s = null; // no schema
        String headerValue = "example-doc-id";
        Map<String, String> value = new HashMap<>();
        value.put("hello", "world");
        SinkRecord sr = new SinkRecord("test", 13, null, "0001", s, value, 0);
        sr.headers().addString(HEADER_DOC_ID_KEY, headerValue);
        // when...
        Document converted = mapper.apply(sr);
        // then...
        assertEquals("world", converted.get("hello"));
        assertEquals(headerValue, converted.get("_id"));
    }

    @Test
    public void testConvertToMapNoSchemaWithInvalidHeader() {
        // given...
        Schema s = null; // no schema
        Schema headerSchema = SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA);
        Map<String, String> headerValue = new HashMap<>();
        headerValue.put("invalid", "value");
        Map<String, String> value = new HashMap<>();
        value.put("_id", "foo");
        value.put("hello", "world");
        SinkRecord sr = new SinkRecord("test", 13, null, "0001", s, value, 0);
        sr.headers().addMap(HEADER_DOC_ID_KEY, headerValue, headerSchema);
        // when...
        Document converted = mapper.apply(sr);
        // then...
        assertEquals("world", converted.get("hello"));
        assertEquals("foo", converted.get("_id"));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testConvertComplexStruct() {
        // given...
        Schema s = SchemaBuilder.struct()
                .field("_id", Schema.STRING_SCHEMA)
                .field("_rev", Schema.STRING_SCHEMA)
                .field("boolean", Schema.BOOLEAN_SCHEMA)
                .field("double", Schema.FLOAT64_SCHEMA)
                .field("float", Schema.FLOAT32_SCHEMA)
                .field("integer", Schema.INT32_SCHEMA)
                .field("long", Schema.INT64_SCHEMA)
                .field("null", Schema.OPTIONAL_STRING_SCHEMA)
                .field("string", Schema.STRING_SCHEMA)
                .field("struct", SchemaBuilder.struct()
                        .field("string_1", Schema.STRING_SCHEMA)
                        .field("map", SchemaBuilder.map(SchemaBuilder.STRING_SCHEMA,
                                SchemaBuilder.struct().field("string_2", Schema.STRING_SCHEMA).build()))
                        .build());

        // build a complex structure according to the above schema:
        // Struct{_id=foo1,_rev=foo2,boolean=true,double=3.14,float=3.14,integer=42,long=42,string=foo3,struct=Struct{string_1=foo4,map={struct2=Struct{string_2=foo5}}}}
        Struct value = new Struct(s);
        value.put("_id", "foo1");
        value.put("_rev", "foo2");
        value.put("boolean", true);
        value.put("double", 3.14d);
        value.put("float", 3.14f);
        value.put("integer", 42);
        value.put("long", 42l);
        value.put("null", null);
        value.put("string", "foo3");
        Struct innerStruct = new Struct(s.field("struct").schema());
        innerStruct.put("string_1", "foo4");
        Map<String, Object> innerMap = new HashMap<>();
        Struct innerInnerStruct = new Struct(s.field("struct").schema().field("map").schema().valueSchema());
        innerInnerStruct.put("string_2", "foo5");
        innerMap.put("struct2", innerInnerStruct);
        innerStruct.put("map", innerMap);
        value.put("struct", innerStruct);

        // when...
        try {
            value.validate();
        } catch (DataException de) {
            fail("Data invalid according to schema");
        }

        // do conversion
        SinkRecord sr = new SinkRecord("test", 13, null, "0001", s, value, 0);
        Document converted = mapper.apply(sr);

        // then...
        assertEquals("foo1", converted.get("_id"));
        assertEquals("foo2", converted.get("_rev"));
        assertEquals(true, converted.get("boolean"));
        assertEquals(3.14d, converted.get("double"));
        assertEquals(3.14f, converted.get("float"));
        assertEquals(42, converted.get("integer"));
        assertEquals(42l, converted.get("long"));
        assertEquals(null, converted.get("null"));
        assertEquals("foo3", converted.get("string"));
        assertEquals("foo4", ((Map<String, Object>) converted.get("struct")).get("string_1"));
        assertEquals("foo5", ((Map<String, Object>) ((Map<String, Object>) ((Map<String, Object>) converted.get("struct")).get("map")).get("struct2")).get("string_2"));

    }

    // TODO test for array of structs
    @SuppressWarnings("unchecked")
    @Test
    public void testConvertArrayOfStructs() {
        // given...
        Schema s = SchemaBuilder.struct()
                .field("struct_array", SchemaBuilder.array(SchemaBuilder.struct().field("string", Schema.STRING_SCHEMA).build()).build());

        // build a complex structure according to the above schema:
        // Struct[struct=Struct{string=foo1},struct=Struct{string=foo1}]
        Struct value = new Struct(s);
        ArrayList<Struct> list = new ArrayList<>();
        Schema innerSchema = s.field("struct_array").schema().valueSchema();
        Struct listValue1 = new Struct(innerSchema);
        Struct listValue2 = new Struct(innerSchema);
        listValue1.put("string", "foo1");
        listValue2.put("string", "foo2");
        list.add(listValue1);
        list.add(listValue2);
        value.put("struct_array", list);

        // do conversion
        SinkRecord sr = new SinkRecord("test", 13, null, "0001", s, value, 0);
        Document converted = mapper.apply(sr);


        System.out.println(s);
        assertEquals(((List<Map<String, Object>>) converted.get("struct_array")).get(0).get("string"), "foo1");
        assertEquals(((List<Map<String, Object>>) converted.get("struct_array")).get(1).get("string"), "foo2");

        // when...
        try {
            value.validate();
        } catch (DataException de) {
            fail("Data invalid according to schema");
        }
    }

    @Test
    public void testConvertStructOverwriteIdWithHeader() {
        // given...
        String headerValue = "example-doc-id";
        Schema s = SchemaBuilder.struct()
                .field("_id", Schema.STRING_SCHEMA)
                .field("_rev", Schema.STRING_SCHEMA)
                .build();

        Struct value = new Struct(s);
        value.put("_id", "foo1");
        value.put("_rev", "foo2");

        // when...
        try {
            value.validate();
        } catch (DataException de) {
            fail("Data invalid according to schema");
        }

        // do conversion
        SinkRecord sr = new SinkRecord("test", 13, null, "0001", s, value, 0);
        sr.headers().addString(HEADER_DOC_ID_KEY, headerValue);
        Document converted = mapper.apply(sr);

        // then...
        assertEquals(headerValue, converted.get("_id"));
        assertEquals("foo2", converted.get("_rev"));
    }

    @Test
    public void testConvertStructWithHeader() {
        // given...
        String headerValue = "example-doc-id";
        Schema s = SchemaBuilder.struct()
                .field("_rev", Schema.STRING_SCHEMA)
                .build();

        Struct value = new Struct(s);
        value.put("_rev", "foo2");

        // when...
        try {
            value.validate();
        } catch (DataException de) {
            fail("Data invalid according to schema");
        }

        // do conversion
        SinkRecord sr = new SinkRecord("test", 13, null, "0001", s, value, 0);
        sr.headers().addString(HEADER_DOC_ID_KEY, headerValue);
        Document converted = mapper.apply(sr);

        // then...
        assertEquals(headerValue, converted.get("_id"));
        assertEquals("foo2", converted.get("_rev"));
    }

    @Test
    public void testConvertStructWithInvalidHeader() {
        // given...
        int headerValue = 100;
        Schema s = SchemaBuilder.struct()
                .field("_rev", Schema.STRING_SCHEMA)
                .build();

        Struct value = new Struct(s);
        value.put("_rev", "foo2");

        // when...
        try {
            value.validate();
        } catch (DataException de) {
            fail("Data invalid according to schema");
        }

        // do conversion
        SinkRecord sr = new SinkRecord("test", 13, null, "0001", s, value, 0);
        sr.headers().addInt(HEADER_DOC_ID_KEY, headerValue);
        Document converted = mapper.apply(sr);

        // then...
        assertNull(converted.get("_id"));
        assertEquals("foo2", converted.get("_rev"));
    }

    @Test
    public void testConvertStringFails() {
        Schema s = null; // no schema
        String value = "hello";
        SinkRecord sr = new SinkRecord("test", 13, null, "0001", s, value, 0);
        try {
            mapper.apply(sr);
            Assert.fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException iae) {
        }
    }
}
