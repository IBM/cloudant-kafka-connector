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
package com.ibm.cloud.cloudant.kafka.schema;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.ibm.cloud.cloudant.kafka.common.MessageKey;
import com.ibm.cloud.cloudant.kafka.common.utils.ResourceBundleUtil;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A class for converting a JsonArray into a Schema.Type.ARRAY
 */
public class JsonArrayAsSchemaArray extends
        JsonArrayConverter<JsonCollectionConverter.SchemaValue> {

    protected JsonArrayAsSchemaArray(JsonArray array) {
        super(array);
    }

    @Override
    protected SchemaValue generate() {
        // If there is no element, i.e. an empty array, then we assume String type elements.
        if (fields.size() == 0) {
            return new SchemaValue(SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA),
                    Collections.EMPTY_LIST);
        }

        // Otherwise check the schemas because we can't do mixed type arrays in Kafka's
        // Schema.Type.ARRAY
        List<Schema> schemas = fields.values().stream().map(sv -> sv.schema).distinct().collect
                (Collectors.toList());
        Schema schemaToUse;
        boolean remappingRequired = false;
        if (schemas.size() > 1) {
            // There were multiple schemas in the array
            if (schemas.stream().allMatch(s -> Schema.Type.STRUCT == s.type())) {
                // All were structs, but they didn't match
                // We can merge the schemas for all the struct types in the array
                schemaToUse = mergeStructSchemas(schemas);
                // However, a new merged schema means we need to re-evaluate all the values to be
                // structs created from the new merged schema
                remappingRequired = true;
            } else {
                // The schemas were not all of the same type, this isn't supported in Kafka arrays
                throw new IllegalArgumentException(ResourceBundleUtil.get(MessageKey
                        .CLOUDANT_STRUCT_SCHEMA_JSON_MIXED_ARRAY));
            }
        } else {
            // There was one distinct schema
            schemaToUse = schemas.get(0);
        }
        Stream<Object> valueStream = fields.values().stream().map(sv -> sv.value);
        if (remappingRequired) {
            valueStream = valueStream.map(v -> remapStruct(schemaToUse, (Struct) v));
        }
        return new SchemaValue(SchemaBuilder.array(schemaToUse).optional().build(), valueStream
                .collect(Collectors.toList()));
    }

    @Override
    protected void process(String key, JsonObject object) {
        processObjectAsSchemaStruct(key, object);
    }

    @Override
    protected void process(String key, JsonArray array) {
        processArrayAsSchemaArray(key, array);
    }

    /**
     * This method merges a series of Struct schemas into a single schema. This is used for example
     * for arrays like: [{"a":1, "b":2}, {"a":1,"c":3}, {"a":1, "b":2, "c":3}]
     * The elements are of the same type (Struct) but we can only use the most complex Struct to
     * describe the overall schema. If we used the schema of the first element we would not be
     * able to assign values for the property "c" in the later objects. In this example the schema
     * of the third element would be sufficient, but a full merge is required to account for complex
     * cases including object nesting.
     *
     * @param structSchemasToMerge list of Struct schemas to merge into one
     * @return a merged Struct schema
     */
    private Schema mergeStructSchemas(List<Schema> structSchemasToMerge) {
        // i) flat map to extract all the Fields from the schemas
        // ii) Group those Fields by name (i.e. name:List<Field>)
        Map<String, List<Field>> fieldGroups = structSchemasToMerge.stream()
                .flatMap(s -> s.fields().stream())
                .collect(Collectors.groupingBy(Field::name));
        // iii) map from a List<Field> to a List<Schema>
        // iv) Distinct Schemas only to merge all the identical field Schemas
        // v) Collect into a map of name:List<Schema>
        Map<String, List<Schema>> schemaGroups = fieldGroups.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e ->
                        e.getValue().stream().map(Field::schema).distinct().collect(Collectors
                                .toList())));
        // vi) Partition by the size of the list to get two groups:
        // field names that have a single Schema
        // field names that have multiple Schemas
        Map<Boolean, List<Map.Entry<String, List<Schema>>>> schemaGroupPartitions = schemaGroups
                .entrySet().stream().collect(Collectors.partitioningBy(e -> e.getValue().size() >
                        1));

        // We want the merged Struct schema to be sorted so use a TreeMap
        // Add all the fields that had a single Schema
        Map<String, Schema> fieldSchemas = new TreeMap<>(schemaGroupPartitions.get(false).stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().get(0).schema())));

        // Merge the schemas for fields that had multiple schemas (these indicate nested structs)
        // We need to recurse to merge those nested structs also and then add the
        // new struct schemas to the fieldSchemas
        schemaGroupPartitions.get(true).forEach(e -> {
            // Add the nested merged schema to the fieldSchemas
            fieldSchemas.put(e.getKey(), mergeStructSchemas(e.getValue()));
        });

        // Now we can build the merged struct schema
        final SchemaBuilder mergedSchemaBuilder = SchemaBuilder.struct().optional();
        fieldSchemas.forEach(mergedSchemaBuilder::field);
        return mergedSchemaBuilder.build();
    }

    /**
     * Takes the fields and values from an existing struct and maps them to a new struct built using
     * a new schema.
     *
     * @param newSchema      the new Schema to use
     * @param existingStruct the existing Struct to convert
     * @return a new Struct built using the new schema
     */
    private Struct remapStruct(Schema newSchema, Struct existingStruct) {
        Struct remappedStruct = new Struct(newSchema);
        for (Field f : existingStruct.schema().fields()) {
            String name = f.name();
            Object value = existingStruct.get(name);
            if (Schema.Type.STRUCT == f.schema().type()) {
                // If the value was a nested struct we need to remap it too
                value = remapStruct(newSchema.field(name).schema(), existingStruct.getStruct(name));
            }
            remappedStruct.put(f.name(), value);
        }
        return remappedStruct;
    }
}
