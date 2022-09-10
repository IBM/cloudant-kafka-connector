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
package com.ibm.cloud.cloudant.kafka.transforms;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.AbstractMap.SimpleEntry;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.Requirements;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.ibm.cloud.cloudant.kafka.common.MessageKey;
import com.ibm.cloud.cloudant.kafka.common.utils.ResourceBundleUtil;

public class ArrayFlatten implements Transformation<SourceRecord> {

    private static final Logger LOG = LoggerFactory.getLogger(ArrayFlatten.class);

    static final String DEFAULT_DELIMITER = ".";
    static final String DELIMITER_CONFIG_NAME = "delimiter";
    private String delimiter = DEFAULT_DELIMITER;

    private static final ConfigDef CONFIG = new ConfigDef()
            .define(DELIMITER_CONFIG_NAME,
            Type.STRING,
            DEFAULT_DELIMITER,
            new ConfigDef.NonEmptyString(),
            ConfigDef.Importance.MEDIUM,
            ResourceBundleUtil.get(MessageKey.CLOUDANT_ARRAY_DELIMITER_DOC));

    @Override
    public void configure(Map<String, ?> configs) {
        SimpleConfig config = new SimpleConfig(CONFIG, configs);
        delimiter = config.getString(DELIMITER_CONFIG_NAME);
    }

    @Override
    public SourceRecord apply(SourceRecord record) {
        Object value = record.value();
        try {
            Requirements.requireMapOrNull(value, String.format(ResourceBundleUtil.get(MessageKey.CLOUDANT_ARRAY_PURPOSE), this.getClass().getName()));
        } catch (DataException de) {
            // The record is not a map/null so we can't flatten and just log dropping the record
            LOG.warn(ResourceBundleUtil.get(MessageKey.CLOUDANT_TRANSFORM_FILTER_RECORD), de);
            return null;
        }
        if (value != null) {
            return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), null, flattenArraysInMap((Map<String, Object>) value), record.timestamp());
        } else {
            return record;
        }
    }

    @Override
    public ConfigDef config() {
        return CONFIG;
    }

    @Override
    public void close() {
        // No cleanup to do
    }

    Map<String, Object> flattenArraysInMap(Map<String, Object> map) {
        return Collections.unmodifiableMap(map.entrySet().stream()
            .flatMap(entry -> {
                return entryToEntryStream(entry);
            })
            .collect(() -> new HashMap<String, Object>(),
                    (collectingMap, entry) -> {
                        collectingMap.put(entry.getKey(), entry.getValue());
                    },
                    (a, b) -> {
                        a.putAll(b);
                    }));
            // Note this custom collector is required to accommodate null values
            // The default Collectors.toMap(...) will NPE on null values (since many maps don't support them)
    }

    Stream<Map.Entry<String, Object>> entryToEntryStream(Map.Entry<String, Object> entry) {
        return keyAndObjectToEntryStream(entry.getKey(), entry.getValue());
    }

    Stream<Map.Entry<String, Object>> keyAndObjectToEntryStream(String key, Object value) {
        if (value != null) {
            Schema.Type schemaType = ConnectSchema.schemaType(value.getClass());
            switch (schemaType) {
                case ARRAY:
                    return keyAndArrayAsEntryStream(key, (List<Object>) value);
                case MAP:
                    return keyAndMapAsEntryStream(key, (Map<String, Object>) value);
                default:
                    break;
            }
        }
        return Stream.of(new SimpleEntry<String, Object>(key, value));
    }

    Stream<Map.Entry<String, Object>> keyAndMapAsEntryStream(String key, Map<String, Object> map) {
        return Stream.of(new SimpleEntry<String, Object>(key, flattenArraysInMap(map)));
    }

    Stream<Map.Entry<String, Object>> keyAndArrayAsEntryStream(String key, List<Object> array) {;
        int index = 0;
        List<Map.Entry<String, Object>> newEntries = new ArrayList<>();
        for (Object element : array) {
            String newKey = new StringBuilder(key).append(delimiter).append(index++).toString();
            newEntries.addAll(keyAndObjectToEntryStream(newKey, element).collect(Collectors.toList()));
        }
        return newEntries.stream();
    }
}
