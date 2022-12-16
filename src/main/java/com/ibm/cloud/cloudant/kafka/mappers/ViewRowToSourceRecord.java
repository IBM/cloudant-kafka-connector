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

import java.util.Collections;
import java.util.Map;
import java.util.function.Function;
import org.apache.kafka.connect.data.SchemaAndValue;

import com.ibm.cloud.cloudant.kafka.utils.InterfaceConst;
import com.ibm.cloud.cloudant.v1.model.ViewResultRow;

import static com.ibm.cloud.cloudant.kafka.tasks.SourceViewTask.START_KEY;

public class ViewRowToSourceRecord extends CloudantBaseKeyRecord<ViewResultRow> {

    private final Map<String, String> sourcePartition;

    public ViewRowToSourceRecord(Map<String, String> sourcePartition) {
        super(sourcePartition.get(InterfaceConst.URL), sourcePartition.get(InterfaceConst.DB));
        this.sourcePartition = sourcePartition;
    }

    @Override
    protected Function<ViewResultRow, Map<String, Object>> offsetFunction() {
        return (row) -> Collections.singletonMap(START_KEY, row.getKey());
    }

    @Override
    protected Function<ViewResultRow, Map<String, String>> partitionFunction() {
        return (row) -> {return this.sourcePartition;};
    }

    @Override
    protected Function<ViewResultRow, SchemaAndValue> keyFunction() {
        // TODO Figure out a good way of doing a key schema here
        return (row) -> {
            return new SchemaAndValue(null, row.getKey());
        };
    }

    @Override
    protected Function<ViewResultRow, SchemaAndValue> valueFunction() {
        return (row) ->{
            return new SchemaAndValue(null, row.getValue());
        };
    }
    
}
