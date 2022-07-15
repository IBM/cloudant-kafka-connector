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
package com.ibm.cloudant.kafka.transforms;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

public class KeyToDocIdTest {

    private KeyToDocId<SinkRecord> transformRecord = new KeyToDocId<>();

    private String recordKey = "transform-key-test";

    @Rule
    public ExpectedException exceptionRule = ExpectedException.none();

    @After
    public void tearDown() {
        transformRecord.close();
    }

    @Test
    public void schemaStructValueWithStringKey() {
        final Schema simpleKeyStructSchema = SchemaBuilder.struct().name("key struct").version(1).doc("doc")
                .field("key", Schema.OPTIONAL_STRING_SCHEMA)
                .build();

        final Schema simpleValueStructSchema = SchemaBuilder.struct().name("value struct").version(1).doc("doc")
                .field("_rev", Schema.STRING_SCHEMA)
                .field("name", Schema.STRING_SCHEMA)
                .build();
        final Struct simpleValueStruct = new Struct(simpleValueStructSchema)
                .put("_rev", "2-545bceb1a85f41cf558241d7caecb2b3")
                .put("name", "cloudant");

        final SinkRecord record = new SinkRecord(
                "test",
                0,
                simpleKeyStructSchema,
                recordKey,
                simpleValueStructSchema,
                simpleValueStruct,
                0);
        final SinkRecord transformedRecord = transformRecord.apply(record);

        assertEquals(simpleValueStructSchema.name(), transformedRecord.valueSchema().name());
        assertEquals(simpleValueStructSchema.version(), transformedRecord.valueSchema().version());
        assertEquals(simpleValueStructSchema.doc(), transformedRecord.valueSchema().doc());

        assertEquals(Schema.STRING_SCHEMA, transformedRecord.valueSchema().field("_id").schema());
        assertEquals(recordKey, ((Struct) transformedRecord.value()).getString("_id"));
        assertEquals(Schema.STRING_SCHEMA, transformedRecord.valueSchema().field("_rev").schema());
        assertEquals("2-545bceb1a85f41cf558241d7caecb2b3", ((Struct) transformedRecord.value()).getString("_rev"));
        assertEquals(Schema.STRING_SCHEMA, transformedRecord.valueSchema().field("name").schema());
        assertEquals("cloudant", ((Struct) transformedRecord.value()).getString("name"));
    }

    @Test
    public void schemaStructValueWithUnsupportedStructKey() {
        final Schema simpleKeyStructSchema = SchemaBuilder.struct().name("key struct").version(1).doc("doc")
                .field("key", Schema.OPTIONAL_STRING_SCHEMA)
                .build();

        final Struct simpleKeyStruct = new Struct(simpleKeyStructSchema).put("key", recordKey);

        final Schema simpleValueStructSchema = SchemaBuilder.struct().name("value struct").version(1).doc("doc")
                .field("_rev", Schema.STRING_SCHEMA)
                .field("name", Schema.STRING_SCHEMA)
                .build();
        final Struct simpleValueStruct = new Struct(simpleValueStructSchema)
                .put("_rev", "2-545bceb1a85f41cf558241d7caecb2b3")
                .put("name", "cloudant");

        final SinkRecord record = new SinkRecord(
                "test",
                0,
                simpleKeyStructSchema,
                simpleKeyStruct,
                simpleValueStructSchema,
                simpleValueStruct,
                0);
        final SinkRecord transformedRecord = transformRecord.apply(record);

        assertEquals(simpleValueStructSchema.name(), transformedRecord.valueSchema().name());
        assertEquals(simpleValueStructSchema.version(), transformedRecord.valueSchema().version());
        assertEquals(simpleValueStructSchema.doc(), transformedRecord.valueSchema().doc());

        assertNull(transformedRecord.valueSchema().field("_id"));
        // assert exception when trying to access _id field that does not exist in schema
        exceptionRule.expect(DataException.class);
        exceptionRule.expectMessage("_id is not a valid field name");
        assertEquals(recordKey, ((Struct) transformedRecord.value()).getString("_id"));

        assertEquals(Schema.STRING_SCHEMA, transformedRecord.valueSchema().field("_rev").schema());
        assertEquals("2-545bceb1a85f41cf558241d7caecb2b3", ((Struct) transformedRecord.value()).getString("_rev"));
        assertEquals(Schema.STRING_SCHEMA, transformedRecord.valueSchema().field("name").schema());
        assertEquals("cloudant", ((Struct) transformedRecord.value()).getString("name"));
    }

    @Test
    public void schemaStructValueWithUnsupportedMapKey() {
        final Schema simpleKeyStructSchema = SchemaBuilder.struct().name("key struct").version(1).doc("doc")
                .field("key", Schema.OPTIONAL_STRING_SCHEMA)
                .build();

        final Map simpleKeyMap = Collections.singletonMap("key", recordKey);

        final Schema simpleValueStructSchema = SchemaBuilder.struct().name("value struct").version(1).doc("doc")
                .field("_rev", Schema.STRING_SCHEMA)
                .field("name", Schema.STRING_SCHEMA)
                .build();
        final Struct simpleValueStruct = new Struct(simpleValueStructSchema)
                .put("_rev", "2-545bceb1a85f41cf558241d7caecb2b3")
                .put("name", "cloudant");

        final SinkRecord record = new SinkRecord(
                "test",
                0,
                simpleKeyStructSchema,
                simpleKeyMap,
                simpleValueStructSchema,
                simpleValueStruct,
                0);
        final SinkRecord transformedRecord = transformRecord.apply(record);

        assertEquals(simpleValueStructSchema.name(), transformedRecord.valueSchema().name());
        assertEquals(simpleValueStructSchema.version(), transformedRecord.valueSchema().version());
        assertEquals(simpleValueStructSchema.doc(), transformedRecord.valueSchema().doc());

        assertNull(transformedRecord.valueSchema().field("_id"));
        // assert exception when trying to access _id field that does not exist in schema
        exceptionRule.expect(DataException.class);
        exceptionRule.expectMessage("_id is not a valid field name");
        assertEquals(recordKey, ((Struct) transformedRecord.value()).getString("_id"));

        assertEquals(Schema.STRING_SCHEMA, transformedRecord.valueSchema().field("_rev").schema());
        assertEquals("2-545bceb1a85f41cf558241d7caecb2b3", ((Struct) transformedRecord.value()).getString("_rev"));
        assertEquals(Schema.STRING_SCHEMA, transformedRecord.valueSchema().field("name").schema());
        assertEquals("cloudant", ((Struct) transformedRecord.value()).getString("name"));
    }

    @Test
    public void schemaStructValueWithNullKey() {
        final Schema simpleKeyStructSchema = SchemaBuilder.struct().name("key struct").version(1).doc("doc")
                .field("key", Schema.OPTIONAL_STRING_SCHEMA)
                .build();

        final Schema simpleValueStructSchema = SchemaBuilder.struct().name("value struct").version(1).doc("doc")
                .field("_rev", Schema.STRING_SCHEMA)
                .field("name", Schema.STRING_SCHEMA)
                .build();
        final Struct simpleValueStruct = new Struct(simpleValueStructSchema)
                .put("_rev", "2-545bceb1a85f41cf558241d7caecb2b3")
                .put("name", "cloudant");

        final SinkRecord record = new SinkRecord(
                "test",
                0,
                simpleKeyStructSchema,
                null,
                simpleValueStructSchema,
                simpleValueStruct,
                0);
        final SinkRecord transformedRecord = transformRecord.apply(record);

        assertEquals(simpleValueStructSchema.name(), transformedRecord.valueSchema().name());
        assertEquals(simpleValueStructSchema.version(), transformedRecord.valueSchema().version());
        assertEquals(simpleValueStructSchema.doc(), transformedRecord.valueSchema().doc());

        assertNull(transformedRecord.key());
        // assert exception when trying to access _id field that does not exist in schema
        exceptionRule.expect(DataException.class);
        exceptionRule.expectMessage("_id is not a valid field name");
        assertEquals(recordKey, ((Struct) transformedRecord.value()).getString("_id"));

        assertEquals(Schema.STRING_SCHEMA, transformedRecord.valueSchema().field("_rev").schema());
        assertEquals("2-545bceb1a85f41cf558241d7caecb2b3", ((Struct) transformedRecord.value()).getString("_rev"));
        assertEquals(Schema.STRING_SCHEMA, transformedRecord.valueSchema().field("name").schema());
        assertEquals("cloudant", ((Struct) transformedRecord.value()).getString("name"));
    }

    @Test
    public void schemaMapValueWithKey() {
        final Schema keySchema = SchemaBuilder.struct().name("key struct").version(1).doc("doc")
                .field("key", Schema.OPTIONAL_STRING_SCHEMA)
                .build();

        final Schema simpleValueMapSchema = SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA)
                .name("value map").version(1).doc("doc")
                .build();

        final Map simpleValueMap = new HashMap<>();
        simpleValueMap.put("_rev", "2-545bceb1a85f41cf558241d7caecb2b3");
        simpleValueMap.put("name", "cloudant");

        final SinkRecord record = new SinkRecord(
                "test",
                0,
                keySchema,
                recordKey,
                simpleValueMapSchema,
                simpleValueMap,
                0);
        final SinkRecord transformedRecord = transformRecord.apply(record);

        assertEquals(simpleValueMapSchema.name(), transformedRecord.valueSchema().name());
        assertEquals(simpleValueMapSchema.version(), transformedRecord.valueSchema().version());
        assertEquals(simpleValueMapSchema.doc(), transformedRecord.valueSchema().doc());

        assertNotNull(transformedRecord.key());
        assertEquals(recordKey, ((Map) transformedRecord.value()).get("_id"));
        assertEquals("2-545bceb1a85f41cf558241d7caecb2b3", ((Map) transformedRecord.value()).get("_rev"));
        assertEquals("cloudant", ((Map) transformedRecord.value()).get("name"));
    }

    @Test
    public void schemaMapValueWithNullKey() {
        final Schema keySchema = SchemaBuilder.struct().name("key struct").version(1).doc("doc")
                .field("key", Schema.OPTIONAL_STRING_SCHEMA)
                .build();

        final Schema simpleValueMapSchema = SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA)
                .name("value map").version(1).doc("doc")
                .build();

        final Map simpleValueMap = new HashMap<>();
        simpleValueMap.put("_rev", "2-545bceb1a85f41cf558241d7caecb2b3");
        simpleValueMap.put("name", "cloudant");

        final SinkRecord record = new SinkRecord(
                "test",
                0,
                keySchema,
                null,
                simpleValueMapSchema,
                simpleValueMap,
                0);
        final SinkRecord transformedRecord = transformRecord.apply(record);

        assertEquals(simpleValueMapSchema.name(), transformedRecord.valueSchema().name());
        assertEquals(simpleValueMapSchema.version(), transformedRecord.valueSchema().version());
        assertEquals(simpleValueMapSchema.doc(), transformedRecord.valueSchema().doc());

        assertNull(transformedRecord.key());
        assertNull(((Map) transformedRecord.value()).get("_id"));
        assertEquals("2-545bceb1a85f41cf558241d7caecb2b3", ((Map) transformedRecord.value()).get("_rev"));
        assertEquals("cloudant", ((Map) transformedRecord.value()).get("name"));
    }

    @Test
    public void schemalessValueWithStringKey() {
        final Schema simpleKeyStructSchema = SchemaBuilder.struct().name("key struct").version(1).doc("doc")
                .field("key", Schema.OPTIONAL_STRING_SCHEMA)
                .build();

        final SinkRecord record = new SinkRecord(
                "test",
                0,
                simpleKeyStructSchema,
                recordKey,
                null,
                null,
                0);
        final SinkRecord transformedRecord = transformRecord.apply(record);

        assertNotNull(transformedRecord.key());
        assertEquals(recordKey, ((Map) transformedRecord.value()).get("_id"));
    }

    @Test
    public void schemalessValueWithMapValueAndStringKey() {
        final SinkRecord record = new SinkRecord(
                "test",
                0,
                null,
                recordKey,
                null,
                Collections.singletonMap("_rev", "2-545bceb1a85f41cf558241d7caecb2b3"),
                0);

        final SinkRecord transformedRecord = transformRecord.apply(record);
        assertEquals("2-545bceb1a85f41cf558241d7caecb2b3", ((Map) transformedRecord.value()).get("_rev"));
        assertEquals(recordKey, ((Map) transformedRecord.value()).get("_id"));
    }

    @Test
    public void schemalessValueWithNullKey() {
        final Schema simpleKeyStructSchema = SchemaBuilder.struct().name("key struct").version(1).doc("doc")
                .field("key", Schema.OPTIONAL_STRING_SCHEMA)
                .build();

        final SinkRecord record = new SinkRecord(
                "test",
                0,
                simpleKeyStructSchema,
                null,
                null,
                null,
                0);
        final SinkRecord transformedRecord = transformRecord.apply(record);

        assertNull(transformedRecord.key());
        assertNull(((Map) transformedRecord.value()));
    }
}
