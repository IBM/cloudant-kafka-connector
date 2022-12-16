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

import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.source.SourceRecord;

/**
 * I - item type
 */
abstract class ItemToSourceRecord<I> implements BiFunction<String, I, SourceRecord>{

    protected final Function<I, Map<String, String>> partitionFunction;
    protected final Function<I, SchemaAndValue> keyFunction;
    protected final Function<I, SchemaAndValue> valueFunction;
    protected final Function<I, Map<String, Object>> offsetFunction;

    protected ItemToSourceRecord() {
        this.partitionFunction = partitionFunction();
        this.offsetFunction = offsetFunction();
        this.keyFunction = keyFunction();
        this.valueFunction = valueFunction();
    }

    @Override
    public SourceRecord apply(String topic, I item) {
        SchemaAndValue key = keyFunction.apply(item);
        SchemaAndValue value = valueFunction.apply(item);
        return new SourceRecord(partitionFunction.apply(item),
                offsetFunction.apply(item),
                topic,
                key.schema(),
                key.value(),
                value.schema(),
                value.value());
    }

    protected abstract Function<I, Map<String, Object>> offsetFunction();

    protected abstract Function<I, Map<String, String>> partitionFunction();

    protected abstract Function<I, SchemaAndValue> keyFunction();

    protected abstract Function<I, SchemaAndValue> valueFunction();
}
