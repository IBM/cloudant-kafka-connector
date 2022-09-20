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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Test;

public class ArrayFlattenTest {
    
    private static final String MAP_KEY = "nested";
    private static final String ARRAY_KEY = "array";

    ArrayFlatten arrayFlattenTransform = new ArrayFlatten();

    private SourceRecord wrapInRecord(Map<String, Object> doc) {
        return new SourceRecord(Collections.emptyMap(), Collections.emptyMap(), "test", null, null, null, doc);
    }

    private Map<String, Object> getFlattened(Map<String, Object> doc) {
        SourceRecord flattened = arrayFlattenTransform.apply(wrapInRecord(doc));
        return (Map<String, Object>) flattened.value();
    }

    private void assertEntries(Map<String, Object> flattenedMap, String key, List<Object> array, String expectedDelimiter) {
        int index = 0;
        for (Object element : array) {
            String keyToAssert = key + expectedDelimiter + index;
            assertEquals(
                String.format("The map entry %s should be as expected", keyToAssert),
                element,
                flattenedMap.get(keyToAssert));
            index++;
        }
    }

    private void assertEntries(Map<String, Object> flattenedMap, String key, List<Object> array) {
        assertEntries(flattenedMap, key, array, ArrayFlatten.DEFAULT_DELIMITER);
    }

    @Test
    public void testNullRecordValue() {
        Map<String, Object> doc = null;
        Map<String, Object> flattenedDoc = getFlattened(doc);
        assertNull(flattenedDoc);
    }  

    @Test
    public void testNoArray() {
        Map<String, Object> doc = Collections.singletonMap("no_array", "foo");
        Map<String, Object> flattenedDoc = getFlattened(doc);
        assertNotNull(flattenedDoc);
        assertEquals("The map should be equal to the original if there are no arrays.", doc, flattenedDoc);
    }    

    @Test
    public void testArray() {
        List<Object> testArray = Arrays.asList("foo0", "foo1");
        Map<String, Object> doc = Collections.singletonMap(ARRAY_KEY, testArray);
        Map<String, Object> flattenedDoc = getFlattened(doc);
        assertNotNull(flattenedDoc);
        assertNotEquals("The map should not be equal to the original map", doc, flattenedDoc);
        assertNull("The array element should have been removed from the flattened map", flattenedDoc.get(ARRAY_KEY));
        assertEntries(flattenedDoc, ARRAY_KEY, testArray);
    }

    @Test
    public void testArrayNestedInArray() {
        List<Object> testArray0 = Arrays.asList("foo0.0", "foo0.1");
        List<Object> testArray1 = Arrays.asList("foo1.0", "foo1.1");
        Map<String, Object> doc = Collections.singletonMap(ARRAY_KEY, Arrays.asList(testArray0, testArray1));
        Map<String, Object> flattenedDoc = getFlattened(doc);
        assertNotNull(flattenedDoc);
        assertNotEquals("The map should not be equal to the original map", doc, flattenedDoc);
        assertNull("The array element should have been removed from the flattened map", flattenedDoc.get(ARRAY_KEY));
        assertEntries(flattenedDoc, ARRAY_KEY + ArrayFlatten.DEFAULT_DELIMITER + "0", testArray0);
        assertEntries(flattenedDoc, ARRAY_KEY + ArrayFlatten.DEFAULT_DELIMITER + "1", testArray1);
    }

    @Test
    public void testArrayNestedInMap() {
        List<Object> testArray = Arrays.asList("foo0", "foo1");
        Map<String, Object> doc = Collections.singletonMap(MAP_KEY, Collections.singletonMap(ARRAY_KEY, testArray));
        Map<String, Object> flattenedDoc = getFlattened(doc);
        assertNotNull(flattenedDoc);
        assertNotEquals("The map should not be equal to the original map", doc, flattenedDoc);
        Map<String, Object> nestedMap = (Map<String, Object>) flattenedDoc.get(MAP_KEY);
        assertNotNull("The nested map should be present", nestedMap);
        assertNull("The array element should have been removed from the nested flattened map", nestedMap.get(ARRAY_KEY));
        assertEntries(nestedMap, ARRAY_KEY, testArray);
    }

    @Test
    public void testMixedTypeArray() {
        List<Object> testArray = Arrays.asList("foo0", true, 7, Long.MAX_VALUE, 3.14159f);
        Map<String, Object> doc = Collections.singletonMap(ARRAY_KEY, testArray);
        Map<String, Object> flattenedDoc = getFlattened(doc);
        assertNotNull(flattenedDoc);
        assertNotEquals("The map should not be equal to the original map", doc, flattenedDoc);
        assertNull("The array element should have been removed from the flattened map", flattenedDoc.get(ARRAY_KEY));
        assertEntries(flattenedDoc, ARRAY_KEY, testArray);
    }

    @Test
    public void testEmptyArray() {
        List<Object> testArray = Collections.emptyList();
        Map<String, Object> doc = Collections.singletonMap(ARRAY_KEY, testArray);
        Map<String, Object> flattenedDoc = getFlattened(doc);
        assertNotNull(flattenedDoc);
        assertNotEquals("The map should not be equal to the original map", doc, flattenedDoc);
        assertNull("The array element should have been removed from the flattened map", flattenedDoc.get(ARRAY_KEY));
        assertEntries(flattenedDoc, ARRAY_KEY, testArray);
    }

    @Test
    public void testArrayWithNullElement() {
        List<Object> testArray = Collections.singletonList(null);
        Map<String, Object> doc = Collections.singletonMap(ARRAY_KEY, testArray);
        Map<String, Object> flattenedDoc = getFlattened(doc);
        assertNotNull(flattenedDoc);
        assertNotEquals("The map should not be equal to the original map", doc, flattenedDoc);
        assertNull("The array element should have been removed from the flattened map", flattenedDoc.get(ARRAY_KEY));
        assertEntries(flattenedDoc, ARRAY_KEY, testArray);
    }

    @Test
    public void testArrayNestedInMapNestedInArray() {
        List<Object> testArray = Arrays.asList("foo0", "foo1");
        Map<String, Object> doc = Collections.singletonMap(MAP_KEY, Collections.singletonList(Collections.singletonMap(ARRAY_KEY, testArray)));
        Map<String, Object> flattenedDoc = getFlattened(doc);
        assertNotNull(flattenedDoc);
        assertNotEquals("The map should not be equal to the original map", doc, flattenedDoc);
        assertNull("The array element should have been removed from the flattened map", flattenedDoc.get(MAP_KEY));
        // Map nested in array should now be an element in the flattened map
        Map<String, Object> nestedMap = (Map<String, Object>) flattenedDoc.get(MAP_KEY + ArrayFlatten.DEFAULT_DELIMITER + "0");
        assertNotNull("The nested map should be present", nestedMap);
        assertNull("The array element should have been removed from the nested flattened map", nestedMap.get(ARRAY_KEY));
        assertEntries(nestedMap, ARRAY_KEY, testArray);
    }

    @Test
    public void testCustomDelimiter() {
        String customDelimiter = "-";
        arrayFlattenTransform.configure(Collections.singletonMap(ArrayFlatten.DELIMITER_CONFIG_NAME, customDelimiter));
        List<Object> testArray = Arrays.asList("foo0", "foo1");
        Map<String, Object> doc = Collections.singletonMap(ARRAY_KEY, testArray);
        Map<String, Object> flattenedDoc = getFlattened(doc);
        assertNotNull(flattenedDoc);
        assertNotEquals("The map should not be equal to the original map", doc, flattenedDoc);
        assertNull("The array element should have been removed from the flattened map", flattenedDoc.get(ARRAY_KEY));
        assertEntries(flattenedDoc, ARRAY_KEY, testArray, customDelimiter);
    }

    @Test
    public void testIncompatibleRecord() {
        String badValue = "notAMap";
        SourceRecord badValueRecord = new SourceRecord(Collections.emptyMap(), Collections.emptyMap(), "test", null, null, null, badValue);
        // Assert null from flatten, which means filtering the bad record.
        assertNull(arrayFlattenTransform.apply(badValueRecord));
    }
}
