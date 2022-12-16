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
import com.ibm.cloud.cloudant.kafka.utils.InterfaceConst;
import com.ibm.cloud.cloudant.v1.model.ChangesResultItem;
import com.ibm.cloud.cloudant.v1.model.Document;

public class ChangesResultItemToSourceRecord extends DocumentBasedRecord<ChangesResultItem> {

    // Note this is a unique identifier for the Cloudant source.
    // For changes, at present, we consider a DB the source partition and
    // uniquely identify it by URL and name.
    private final Map<String, String> sourcePartition;

    public ChangesResultItemToSourceRecord(Map<String, String> sourcePartition) {
        super(sourcePartition.get(InterfaceConst.URL), sourcePartition.get(InterfaceConst.DB));
        this.sourcePartition = sourcePartition;
    }

    @Override
    protected Function<ChangesResultItem, Map<String, Object>> offsetFunction() {
        return (item) -> {
            return Collections.singletonMap(InterfaceConst.LAST_CHANGE_SEQ, item.getSeq());
        };
    }

    @Override
    protected Function<ChangesResultItem, Map<String, String>> partitionFunction() {
        return (item) -> {
            return this.sourcePartition;
        };
    }

    @Override
    String getDocIdFromItem(ChangesResultItem item) {
        return item.getId();
    }

    @Override
    Document getDocumentFromItem(ChangesResultItem item) {
        return item.getDoc();
    }
    
}
