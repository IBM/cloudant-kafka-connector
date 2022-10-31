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
package com.ibm.cloud.cloudant.kafka.transforms.predicates;

import static com.ibm.cloud.cloudant.kafka.mappers.DocumentToSourceRecord.RECORD_KEY_SCHEMA;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import java.util.Collections;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Test;
import com.ibm.cloud.cloudant.kafka.utils.CloudantConst;

public class IsDesignDocumentTest {

    IsDesignDocument isDDoc = new IsDesignDocument();
    private SourceRecord wrapInRecord(Schema keySchema, Object key) {
        return new SourceRecord(Collections.emptyMap(), Collections.emptyMap(), "test", keySchema, key, null, null);
    }

    @Test(expected = DataException.class)
    public void testNullSchemaAndKey() {
        isDDoc.test(wrapInRecord(null, null));
    }

    @Test(expected = DataException.class)
    public void testNullKeyValue() {
        isDDoc.test(wrapInRecord(RECORD_KEY_SCHEMA, null));
    }

    @Test(expected = DataException.class)
    public void testWrongKeyType() {
        isDDoc.test(wrapInRecord(Schema.STRING_SCHEMA, "foo"));
    }

    @Test(expected = DataException.class)
    public void testKeyNoId() {
        // Struct.put will throw exception, no need to call isDDoc.test()
        new Struct(RECORD_KEY_SCHEMA).put("foo", "bar");
    }

    @Test
    public void testDesignDocument() {
        assertTrue("The design document should be filtered.",
            isDDoc.test(wrapInRecord(RECORD_KEY_SCHEMA, new Struct(RECORD_KEY_SCHEMA).put(CloudantConst.CLOUDANT_DOC_ID, "_design/foo"))));
    }

    @Test
    public void testNearlyDesignDocument() {
        assertFalse("The document should not be filtered.",
            isDDoc.test(wrapInRecord(RECORD_KEY_SCHEMA, new Struct(RECORD_KEY_SCHEMA).put(CloudantConst.CLOUDANT_DOC_ID, "_designfoo"))));
    }

    @Test
    public void testNotDesignDocument() {
        assertFalse("The document should not be filtered.",
            isDDoc.test(wrapInRecord(RECORD_KEY_SCHEMA, new Struct(RECORD_KEY_SCHEMA).put(CloudantConst.CLOUDANT_DOC_ID, "foo"))));
    }
}
