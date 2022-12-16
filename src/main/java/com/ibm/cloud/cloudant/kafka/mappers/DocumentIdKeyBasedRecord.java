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

import java.util.function.Function;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import com.ibm.cloud.cloudant.kafka.utils.CloudantConst;

public abstract class DocumentIdKeyBasedRecord<I> extends CloudantBaseKeyRecord<I> {

        DocumentIdKeyBasedRecord(String url, String database) {
                super(url, database);
        }

        // Record key schema is {"_id": doc_id, "cloudant.url": url, "cloudant.db": db}
        public static final Schema RECORD_KEY_SCHEMA = addDatabaseAndUrlToStructSchema(
                SchemaBuilder.struct().field(CloudantConst.CLOUDANT_DOC_ID, Schema.STRING_SCHEMA)).build();
    
        @Override
        protected Function<I, SchemaAndValue> keyFunction() {
                return (item) -> {
                        return new SchemaAndValue(RECORD_KEY_SCHEMA,
                                addDatabaseAndUrlToStruct(new Struct(RECORD_KEY_SCHEMA)
                                .put(CloudantConst.CLOUDANT_DOC_ID, getDocIdFromItem(item))));
                };
        }

        abstract String getDocIdFromItem(I item);
}
