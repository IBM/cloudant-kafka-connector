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
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Collections;
import java.util.Map;

import static org.junit.Assert.*;

public class KeyToDocIdTest {

    private KeyToDocId<SourceRecord> transformRecord = new KeyToDocId<>();

    private String recordKey = "transform-key-test";

    @After
    public void tearDown() {
        transformRecord.close();
    }

    @Test
    public void schemaCopyRecordKeyToIdValueField() {
        final Schema simpleKeyStructSchema = SchemaBuilder.struct().name("key struct").version(1).doc("doc")
                .field("key", Schema.OPTIONAL_STRING_SCHEMA)
                .build();

        final Struct simpleKeyStruct = new Struct(simpleKeyStructSchema).put("key", recordKey);

        final Schema simpleValueStructSchema = SchemaBuilder.struct().name("value struct").version(1).doc("doc")
                .field("_id", Schema.OPTIONAL_STRING_SCHEMA)
                .field("_rev", Schema.STRING_SCHEMA)
                .field("name", Schema.STRING_SCHEMA)
                .build();
        final Struct simpleValueStruct = new Struct(simpleValueStructSchema)
                .put("_rev", "2-545bceb1a85f41cf558241d7caecb2b3")
                .put("name", "cloudant");

        final SourceRecord record = new SourceRecord(
                null,
                null,
                "test",
                0,
                simpleKeyStructSchema,
                simpleKeyStruct,
                simpleValueStructSchema,
                simpleValueStruct);
        final SourceRecord transformedRecord = transformRecord.apply(record);

        assertEquals(simpleValueStructSchema.name(), transformedRecord.valueSchema().name());
        assertEquals(simpleValueStructSchema.version(), transformedRecord.valueSchema().version());
        assertEquals(simpleValueStructSchema.doc(), transformedRecord.valueSchema().doc());

        assertEquals(Schema.OPTIONAL_STRING_SCHEMA, transformedRecord.valueSchema().field("_id").schema());
        assertNotNull(((Struct) transformedRecord.value()).getString("_id"));
        assertEquals(Schema.STRING_SCHEMA, transformedRecord.valueSchema().field("_rev").schema());
        assertEquals("2-545bceb1a85f41cf558241d7caecb2b3", ((Struct) transformedRecord.value()).getString("_rev"));
        assertEquals(Schema.STRING_SCHEMA, transformedRecord.valueSchema().field("name").schema());
        assertEquals("cloudant", ((Struct) transformedRecord.value()).getString("name"));
    }

    @Test
    public void schemaNullRecordKey() {
        final Schema simpleKeyStructSchema = SchemaBuilder.struct().name("key struct").version(1).doc("doc")
                .field("key", Schema.OPTIONAL_STRING_SCHEMA)
                .build();

        final Struct simpleKeyStruct = new Struct(simpleKeyStructSchema).put("key", null);

        final Schema simpleValueStructSchema = SchemaBuilder.struct().name("value struct").version(1).doc("doc")
                .field("_id", Schema.OPTIONAL_STRING_SCHEMA)
                .field("_rev", Schema.STRING_SCHEMA)
                .field("name", Schema.STRING_SCHEMA)
                .build();
        final Struct simpleValueStruct = new Struct(simpleValueStructSchema)
                .put("_rev", "2-545bceb1a85f41cf558241d7caecb2b3")
                .put("name", "cloudant");

        final SourceRecord record = new SourceRecord(
                null,
                null,
                "test",
                0,
                simpleKeyStructSchema,
                simpleKeyStruct,
                simpleValueStructSchema,
                simpleValueStruct);
        final SourceRecord transformedRecord = transformRecord.apply(record);

        assertEquals(simpleValueStructSchema.name(), transformedRecord.valueSchema().name());
        assertEquals(simpleValueStructSchema.version(), transformedRecord.valueSchema().version());
        assertEquals(simpleValueStructSchema.doc(), transformedRecord.valueSchema().doc());

        assertEquals("Struct{}", transformedRecord.key().toString());
        assertEquals(Schema.OPTIONAL_STRING_SCHEMA, transformedRecord.valueSchema().field("_id").schema());
        assertNull(((Struct) transformedRecord.value()).getString("_id"));
        assertEquals(Schema.STRING_SCHEMA, transformedRecord.valueSchema().field("_rev").schema());
        assertEquals("2-545bceb1a85f41cf558241d7caecb2b3", ((Struct) transformedRecord.value()).getString("_rev"));
        assertEquals(Schema.STRING_SCHEMA, transformedRecord.valueSchema().field("name").schema());
        assertEquals("cloudant", ((Struct) transformedRecord.value()).getString("name"));
    }

    @Test
    public void schemaNoRecordKey() {
        final Schema simpleValueStructSchema = SchemaBuilder.struct().name("value struct").version(1).doc("doc")
                .field("_id", Schema.OPTIONAL_STRING_SCHEMA)
                .field("_rev", Schema.STRING_SCHEMA)
                .field("name", Schema.STRING_SCHEMA)
                .build();
        final Struct simpleValueStruct = new Struct(simpleValueStructSchema)
                .put("_rev", "2-545bceb1a85f41cf558241d7caecb2b3")
                .put("name", "cloudant");

        final SourceRecord record = new SourceRecord(
                null,
                null,
                "test",
                0,
                simpleValueStructSchema,
                simpleValueStruct);
        final SourceRecord transformedRecord = transformRecord.apply(record);

        assertEquals(simpleValueStructSchema.name(), transformedRecord.valueSchema().name());
        assertEquals(simpleValueStructSchema.version(), transformedRecord.valueSchema().version());
        assertEquals(simpleValueStructSchema.doc(), transformedRecord.valueSchema().doc());

        assertNull(transformedRecord.key());
        assertEquals(Schema.OPTIONAL_STRING_SCHEMA, transformedRecord.valueSchema().field("_id").schema());
        assertNull(((Struct) transformedRecord.value()).getString("_id"));
        assertEquals(Schema.STRING_SCHEMA, transformedRecord.valueSchema().field("_rev").schema());
        assertEquals("2-545bceb1a85f41cf558241d7caecb2b3", ((Struct) transformedRecord.value()).getString("_rev"));
        assertEquals(Schema.STRING_SCHEMA, transformedRecord.valueSchema().field("name").schema());
        assertEquals("cloudant", ((Struct) transformedRecord.value()).getString("name"));
    }

    @Rule
    public ExpectedException exceptionRule = ExpectedException.none();

    @Test
    public void schemaNoIdFieldInSchemaException() {
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

        final SourceRecord record = new SourceRecord(
                null,
                null,
                "test",
                0,
                simpleKeyStructSchema,
                simpleKeyStruct,
                simpleValueStructSchema,
                simpleValueStruct);
        final SourceRecord transformedRecord = transformRecord.apply(record);

        assertEquals(simpleValueStructSchema.name(), transformedRecord.valueSchema().name());
        assertEquals(simpleValueStructSchema.version(), transformedRecord.valueSchema().version());
        assertEquals(simpleValueStructSchema.doc(), transformedRecord.valueSchema().doc());

        assertNull(transformedRecord.valueSchema().field("_id"));
        assertEquals(Schema.STRING_SCHEMA, transformedRecord.valueSchema().field("_rev").schema());
        assertEquals("2-545bceb1a85f41cf558241d7caecb2b3", ((Struct) transformedRecord.value()).getString("_rev"));
        assertEquals(Schema.STRING_SCHEMA, transformedRecord.valueSchema().field("name").schema());
        assertEquals("cloudant", ((Struct) transformedRecord.value()).getString("name"));

        // assert exception when trying to access _id field that does not exist in schema
        exceptionRule.expect(DataException.class);
        exceptionRule.expectMessage("_id is not a valid field name");
        assertEquals(recordKey, ((Struct) transformedRecord.value()).getString("_id"));
    }

    @Test
    public void schemalessCopyMapKeyToIdValueField() {
        final SourceRecord record = new SourceRecord(
                null,
                null,
                "test",
                0,
                null,
                Collections.singletonMap("key", "test"),
                null,
                Collections.singletonMap("_rev", "2-545bceb1a85f41cf558241d7caecb2b3"));

        final SourceRecord transformedRecord = transformRecord.apply(record);
        assertEquals("2-545bceb1a85f41cf558241d7caecb2b3", ((Map) transformedRecord.value()).get("_rev"));
        assertEquals("{key=test}", ((Map) transformedRecord.value()).get("_id"));
    }

    @Test
    public void schemalessCopyKeyToIdValueField() {
        final SourceRecord record = new SourceRecord(
                null,
                null,
                "test",
                0,
                null,
                "test",
                null,
                Collections.singletonMap("_rev", "2-545bceb1a85f41cf558241d7caecb2b3"));

        final SourceRecord transformedRecord = transformRecord.apply(record);
        assertEquals("2-545bceb1a85f41cf558241d7caecb2b3", ((Map) transformedRecord.value()).get("_rev"));
        assertEquals("test", ((Map) transformedRecord.value()).get("_id"));
    }

    @Test
    public void schemalessNoKey() {
        final SourceRecord record = new SourceRecord(
                null,
                null,
                "test",
                0,
                null,
                Collections.singletonMap("_rev", "2-545bceb1a85f41cf558241d7caecb2b3"));

        final SourceRecord transformedRecord = transformRecord.apply(record);
        assertEquals("2-545bceb1a85f41cf558241d7caecb2b3", ((Map) transformedRecord.value()).get("_rev"));
        assertNull(((Map) transformedRecord.value()).get("_id"));
    }
}
