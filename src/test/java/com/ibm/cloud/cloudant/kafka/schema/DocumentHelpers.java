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
package com.ibm.cloud.cloudant.kafka.schema;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Timestamp;
import com.ibm.cloud.cloudant.kafka.transforms.MapToStruct;
import com.ibm.cloud.cloudant.v1.model.Attachment;
import com.ibm.cloud.cloudant.v1.model.Document;
import com.ibm.cloud.cloudant.v1.model.DocumentRevisionStatus;
import com.ibm.cloud.cloudant.v1.model.Revisions;
import com.ibm.cloud.cloudant.v1.model.DocumentRevisionStatus.Status;

public class DocumentHelpers {

    public enum Primitive {

        // Declare these in string comparator order for easier use
        // because Struct ordering is important.
        BIG_DECIMAL(BigDecimal.TEN, v -> Decimal.builder(((BigDecimal) v).scale()).optional().build()),
        BOOLEAN(true),
        BYTES(new byte[]{Byte.MIN_VALUE, Byte.MAX_VALUE}),
        DATE(Date.from(Instant.now()), v -> Timestamp.builder().optional().build()),
        FLOAT32(Float.MAX_VALUE),
        FLOAT64(Double.MIN_VALUE),
        INT16(Short.MAX_VALUE),
        INT32(Integer.MIN_VALUE),
        INT64(Long.MAX_VALUE),
        INT8(Byte.MIN_VALUE),
        NULL(null, v -> MapToStruct.NULL_VALUE_SCHEMA),
        STRING("string");

        private final SchemaAndValue schemaAndValue;

        Primitive(Object value, Function<Object, Schema> schemaConverter) {
            this.schemaAndValue = new SchemaAndValue(schemaConverter.apply(value), value);
        }

        Primitive(Object value) {
            // Use the default schema inferred from the class type
            this(value, v -> SchemaBuilder.type(ConnectSchema.schemaType(v.getClass())).optional().build());
        }

        public Schema getSchema() {
            return this.schemaAndValue.schema();
        }

        public Object getValue() {
            return this.schemaAndValue.value();
        }        
    }

    static final Document EMPTY_DOC = new Document.Builder().build();
    static final Document MIN_DOC = new Document.Builder().id("test").build();
    static final Document ALL_META_DOC = new Document.Builder()
                                                .id("test")
                                                .rev("99-0123456789abcdef0123456789abcdef")
                                                .attachments(Collections.singletonMap("foo", new Attachment.Builder().contentType("text/plain").data("foo".getBytes()).build()))
                                                .conflicts(Collections.singletonList("88-abcdef0123456789abcdef0123456789"))
                                                .deleted(true)
                                                .deletedConflicts(Collections.singletonList("77-abcdefabcdef01234567890123456789"))
                                                .localSeq("42-abc")
                                                .revisions(new Revisions.Builder().start(98).ids(Collections.singletonList("98-9876543210abcdef0123456789abcdef")).build())
                                                .revsInfo(Collections.singletonList(new DocumentRevisionStatus.Builder("77-abcdefabcdef01234567890123456789", Status.DELETED).build()))
                                                .build();
    static final Document ALL_TYPES_DOC = addFromMap(new Document.Builder(), allTypesAsMap()).build();
    static final Document EVERYTHING_DOC = addFromDoc(ALL_META_DOC.newBuilder(), ALL_TYPES_DOC).build();

    public static final Map<String, Object> primitiveTypesAsMap() {
        Map<String, Object> map = new HashMap<>();
        EnumSet.allOf(Primitive.class).forEach(e -> {
            map.put(e.name(), e.getValue());
        });
        return map;
    }

    public static final Map<String, Object> allTypesAsMap() {
        return allTypesAsMap(DocumentHelpers::primitiveTypesAsMap, DocumentHelpers::allTypesAsArray);
    }

    public static final Map<String, Object> allTypesAsMap(Supplier<Map<String, Object>> mapSupplier, Supplier<List<Object>> arraySupplier) {
        Map<String, Object> map = new HashMap<>();
        map.putAll(primitiveTypesAsMap());
        // Add the nested types
        map.put("array", arraySupplier.get());
        map.put("map", mapSupplier.get());
        return map;
    }

    public static final List<Object> allTypesAsArray() {
        List<Object> array = new ArrayList<>();
        // Make an entry of every type
        array.addAll(primitiveTypesAsMap().values());
        // Add a nested map into the array
        array.add(primitiveTypesAsMap());
        // And a nested array
        List<Object> nestedArray = new ArrayList<>();
        nestedArray.addAll(primitiveTypesAsMap().values());
        array.add(nestedArray);
        return Collections.unmodifiableList(array);
    }

    public static final List<Object> arrayOf(Supplier<Object> elementSupplier, int size) {
        return Collections.nCopies(size, elementSupplier.get());
    }

    public static final Document.Builder addFromDoc(Document.Builder b, Document doc) {
        return addFromMap(b, doc.getProperties());
    }

    public static final Document.Builder addFromMap(Document.Builder b, Map<String, Object> props) {
        props.forEach((k, v) -> {b.add(k, v);});
        return b;
    }
    
}
