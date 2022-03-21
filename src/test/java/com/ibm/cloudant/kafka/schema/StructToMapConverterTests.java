package com.ibm.cloudant.kafka.schema;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.HashMap;
import java.util.Map;

import com.ibm.cloudant.kafka.connect.StructToMapConverter;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Assert;
import org.junit.Test;

public class StructToMapConverterTests {

    // for these tests we will operate on SinkRecords, but they could just as easily be SourceRecords - for our purposes they are equivalent
    StructToMapConverter<SinkRecord> converter = new StructToMapConverter<>();

    @Test
    public void testConvertToMapSchema() {
        Schema s = SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA);
        Map<String, String> value = new HashMap<>();
        value.put("hello", "world");
        SinkRecord sr = new SinkRecord("test", 13, null, "0001", s, value, 0);
        Map<String, Object> converted = converter.convert(sr);
        assertEquals("world", converted.get("hello"));

    }

    @Test
    public void testConvertToMapNoSchema() {


        Schema s = null; // no schema
        Map<String, String> value = new HashMap<>();
        value.put("hello", "world");
        SinkRecord sr = new SinkRecord("test", 13, null, "0001", s, value, 0);
        Map<String, Object> converted = converter.convert(sr);
        assertEquals("world", converted.get("hello"));

    }

    @Test
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
        Map<String, Object> innerMap = new HashMap();
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
        Map<String, Object> converted = converter.convert(sr);

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
        assertEquals("foo4", ((Map<String, Object>)converted.get("struct")).get("string_1"));
        assertEquals("foo5", ((Map<String, Object>)((Map<String, Object>)((Map<String, Object>)converted.get("struct")).get("map")).get("struct2")).get("string_2"));

    }

    @Test
    public void testConvertStringFails() {
        Schema s = null; // no schema
        String value = "hello";
        SinkRecord sr = new SinkRecord("test", 13, null, "0001", s, value, 0);
        try {
            Map<String, Object> converted = converter.convert(sr);
            Assert.fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException iae) {

        }
    }


}
