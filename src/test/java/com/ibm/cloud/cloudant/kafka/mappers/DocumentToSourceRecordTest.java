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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import java.util.Collections;
import java.util.Map;
import org.junit.Test;
import com.ibm.cloud.cloudant.kafka.mappers.DocumentToSourceRecord.MetaProperty;
import com.ibm.cloud.cloudant.v1.model.Document;


public class DocumentToSourceRecordTest {
    
    // We don't need real offset functions for this test, just return empty maps
    DocumentToSourceRecord mapper = new DocumentToSourceRecord(Collections.emptyMap(), (s) -> Collections.emptyMap());


    private void assertMetadata(Document expected, Map<String, Object> actual) {
        for (MetaProperty p : MetaProperty.values()) {
            assertEquals(p.getValueFromDoc(expected), actual.get(p.toString()));
        }
    }

    private void assertProperties(Document expected, Map<String, Object> actual) {
        expected.getProperties().forEach((key, value) -> {
            assertEquals(value, actual.get(key));
        });
    }

    private void assertActualMapAgainstExpected(Document expected, Map<String, Object> actual) {
        assertMetadata(expected, actual);
        assertProperties(expected, actual);
    }

    @Test
    public void testEmptyDocument() {
        Map<String, Object> map = mapper.documentToMap(DocumentHelpers.EMPTY_DOC);
        assertNotNull("The map should not be null.", map);
        assertTrue("The map should be empty.", map.isEmpty());
        assertActualMapAgainstExpected(DocumentHelpers.EMPTY_DOC, map);
    }

    @Test
    public void testDocumentWithId() {
        Map<String, Object> map = mapper.documentToMap(DocumentHelpers.MIN_DOC);
        assertNotNull("The map should not be null.", map);
        assertEquals("There should be only 1 entry in the map.", 1, map.entrySet().size());
        assertActualMapAgainstExpected(DocumentHelpers.MIN_DOC, map);
    }

    @Test
    public void testDocumentWithMetadata() {
        Map<String, Object> map = mapper.documentToMap(DocumentHelpers.ALL_META_DOC);
        assertNotNull("The map should not be null.", map);
        assertEquals("There should be 9 entries in the map.", 9, map.entrySet().size());
        assertActualMapAgainstExpected(DocumentHelpers.ALL_META_DOC, map);
    }

    @Test
    public void testDocumentWithAllTypes() {
        Map<String, Object> map = mapper.documentToMap(DocumentHelpers.ALL_TYPES_DOC);
        assertNotNull("The map should not be null.", map);
        assertEquals("There should be the right number of entries in the map.", DocumentHelpers.allTypesAsMap().size(), map.entrySet().size());
        assertActualMapAgainstExpected(DocumentHelpers.ALL_TYPES_DOC, map);
    }

    @Test
    public void testDocumentWithEverything() {
        Map<String, Object> map = mapper.documentToMap(DocumentHelpers.EVERYTHING_DOC);
        assertNotNull("The map should not be null.", map);
        assertEquals("There should be the right number of entries in the map.", DocumentHelpers.allTypesAsMap().size() + 9, map.entrySet().size());
        assertActualMapAgainstExpected(DocumentHelpers.EVERYTHING_DOC, map);
    }
}
