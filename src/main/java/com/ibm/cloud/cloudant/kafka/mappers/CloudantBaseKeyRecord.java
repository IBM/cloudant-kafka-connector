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

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import com.ibm.cloud.cloudant.kafka.utils.InterfaceConst;

abstract class CloudantBaseKeyRecord<I> extends ItemToSourceRecord<I> {
    
    protected final String url;
    protected final String database;

    CloudantBaseKeyRecord(String url, String database) {
        this.url = url;
        this.database = database;
    }

    protected static SchemaBuilder addDatabaseAndUrlToStructSchema(SchemaBuilder builder) {
        return builder
            .field(InterfaceConst.DB, Schema.OPTIONAL_STRING_SCHEMA)
            .field(InterfaceConst.URL, Schema.OPTIONAL_STRING_SCHEMA);
    }

    protected Struct addDatabaseAndUrlToStruct(Struct struct) {
        return struct
            .put(InterfaceConst.DB, this.database)
            .put(InterfaceConst.URL, this.url);
    }
}
