/*
 * Copyright © 2022, 2023 IBM Corp. All rights reserved.
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
package com.ibm.cloud.cloudant.kafka.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.json.JsonConverter;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import com.google.gson.internal.LazilyParsedNumber;

public class NumberSafeMapTest {

    private static final String testKey = "test";
    private static JsonConverter converter = new JsonConverter();

    @BeforeClass
    public static void configure() {
        converter.configure(Collections.singletonMap("schemas.enable", false), false);
    }

    @AfterClass
    public static void teardown() {
        converter.close();
    }

    private <T> Map<String, Object> makeMap(String valueAsString) {
        Map<String, Object> map = new NumberSafeMap();
        map.put(testKey, new LazilyParsedNumber(valueAsString));
        return map;
    }
    
    private <T> void assertExpectedType(Map<String, Object> map, Class<T> expectedType) {   
        Object o = map.get(testKey);
        assertEquals("The value should be of the expected type.", expectedType, o.getClass());
    }

    private void convert(Map<String, Object> map) {
        converter.fromConnectData("test", null, map);
    }

    @Test
    public void testByteNumberMin() {
        Map<String, Object> testMap = makeMap(Byte.toString(Byte.MIN_VALUE));
        assertExpectedType(testMap, Byte.class);
        convert(testMap);
    }

    @Test
    public void testByteNumber() {
        Map<String, Object> testMap = makeMap("3");
        assertExpectedType(testMap, Byte.class);
        convert(testMap);
    }

    @Test
    public void testByteNumberMax() {
        Map<String, Object> testMap = makeMap(Byte.toString(Byte.MAX_VALUE));
        assertExpectedType(testMap, Byte.class);
        convert(testMap);
    }

    @Test
    public void testShortNumberMin() {
        Map<String, Object> testMap = makeMap(Short.toString(Short.MIN_VALUE));
        assertExpectedType(testMap, Short.class);
        convert(testMap);
    }

    @Test
    public void testShortNumber() {
        Map<String, Object> testMap = makeMap("128");
        assertExpectedType(testMap, Short.class);
        convert(testMap);
    }

    @Test
    public void testShortNumberMax() {
        Map<String, Object> testMap = makeMap(Short.toString(Short.MAX_VALUE));
        assertExpectedType(testMap, Short.class);
        convert(testMap);
    }

    @Test
    public void testIntegerNumberMin() {
        Map<String, Object> testMap = makeMap(Integer.toString(Integer.MIN_VALUE));
        assertExpectedType(testMap, Integer.class);
        convert(testMap);
    }

    @Test
    public void testIntegerNumber() {
        Map<String, Object> testMap = makeMap(Integer.valueOf(Short.MAX_VALUE + 1).toString());
        assertExpectedType(testMap, Integer.class);
        convert(testMap);
    }

    @Test
    public void testIntegerNumberMax() {
        Map<String, Object> testMap = makeMap(Integer.toString(Integer.MAX_VALUE));
        assertExpectedType(testMap, Integer.class);
        convert(testMap);
    }

    @Test
    public void testLongNumberMin() {
        Map<String, Object> testMap = makeMap(Long.toString(Long.MIN_VALUE));
        assertExpectedType(testMap, Long.class);
        convert(testMap);
    }

    @Test
    public void testLongNumber() {
        Map<String, Object> testMap = makeMap(Long.toString(Integer.MAX_VALUE + 1L));
        assertExpectedType(testMap, Long.class);
        convert(testMap);
    }

    @Test
    public void testLongNumberMax() {
        Map<String, Object> testMap = makeMap(Long.toString(Long.MAX_VALUE));
        assertExpectedType(testMap, Long.class);
        convert(testMap);
    }

    @Test
    public void testFloatNumberMin() {
        Map<String, Object> testMap = makeMap(BigDecimal.valueOf(Float.MIN_VALUE).negate().toString());
        assertExpectedType(testMap, Float.class);
        convert(testMap);
    }

    @Test
    public void testFloatNumber() {
        Map<String, Object> testMap = makeMap("0.125");
        assertExpectedType(testMap, Float.class);
        convert(testMap);
    }

    /**
     * Test a Float at extreme range.
     * Note that Float.MAX_VALUE is an integer, and if specified
     * will return a BigInteger.
     */
    @Test
    public void testFloatNormalNumber() {
        Map<String, Object> testMap = makeMap(BigDecimal.valueOf(Float.MIN_NORMAL).toString());
        assertExpectedType(testMap, Float.class);
        convert(testMap);
    }

    @Test
    public void testDoubleNumberMin() {
        Map<String, Object> testMap = makeMap(BigDecimal.valueOf(Double.MIN_VALUE).toString());
        assertExpectedType(testMap, Double.class);
        convert(testMap);
    }

    @Test
    public void testDoubleNumber() {
        Map<String, Object> testMap = makeMap(BigDecimal.valueOf(Math.PI).toString());
        assertExpectedType(testMap, Double.class);
        convert(testMap);
    }

    /**
     * Test a Double at extreme range.
     * Note that Double.MAX_VALUE is an integer, and if specified
     * will return a BigInteger.
     */
    @Test
    public void testDoubleNormalNumber() {
        Map<String, Object> testMap = makeMap(BigDecimal.valueOf(Double.MIN_NORMAL).toString());
        assertExpectedType(testMap, Double.class);
        convert(testMap);
    }

    /**
     * Validate that we leave BigIntegers as BigIntegers.
     * Note they are not supported by the Kafka JsonConverter.
     */
    @Test
    public void testBigInteger() {
        Map<String, Object> testMap = new NumberSafeMap();
        testMap.put(testKey, BigInteger.TEN);
        assertExpectedType(testMap, BigInteger.class);
    }

    /**
     * Validate that we leave BigDecimals as BigDecimals
     * Note they are not supported by the Kafka JsonConverter.
     */
    @Test
    public void testBigDecimal() {
        Map<String, Object> testMap = new NumberSafeMap();
        testMap.put(testKey, BigDecimal.ONE);
        assertExpectedType(testMap, BigDecimal.class);
    }

    /**
     * Validate that null entries don't cause NPEs.
     */
    @Test
    public void testNullValue() {
        Map<String, Object> testMap = new NumberSafeMap();
        testMap.put(testKey, (Number) null);
        assertNull(testMap.get(testKey));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testNumberNestedInMap() {
        Map<String, Object> nestedMap = Collections.singletonMap(testKey, new LazilyParsedNumber("17"));
        Map<String, Object> testMap = new NumberSafeMap();
        testMap.put("nested", nestedMap);
        Map<String, Object> actualNested = (Map<String, Object>) testMap.get("nested");
        assertNotNull("The nested map should not be null.", actualNested);
        assertExpectedType(actualNested, Byte.class);
        convert(testMap);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testNumberNestedInArray() {
        List<LazilyParsedNumber> nestedList = Collections.singletonList(new LazilyParsedNumber("17"));
        Map<String, Object> testMap = new NumberSafeMap();
        testMap.put("nested", nestedList);
        List<Object> actualNested = (List<Object>) testMap.get("nested");
        assertNotNull("The nested list should not be null.", actualNested);
        assertFalse("The nested list should not be empty.", actualNested.isEmpty());
        assertEquals("The nested list should have one element.", 1, actualNested.size());
        assertEquals("The value should be of the expected type.", Byte.class, actualNested.get(0).getClass());
        convert(testMap);
    }

    /**
     * Checks a Number in a Map in a List in a Map in a Map.
     */
    @Test
    @SuppressWarnings("unchecked")
    public void testNumberInDeeperNesting() {
        String value = "255";
        Short expectedValue = Short.valueOf(value);
        Map<String, Object> sourceMap = Collections.singletonMap("outer", 
            Collections.singletonMap("list",
                Collections.singletonList(
                    Collections.singletonMap(testKey, new LazilyParsedNumber(value)))));
        NumberSafeMap actualMap = new NumberSafeMap(sourceMap);
        Map<String, Object> outer = (Map<String, Object>) actualMap.get("outer");
        assertNotNull("The outer map should not be null.", outer);
        List<Object> list = (List<Object>) outer.get("list");
        assertNotNull("The nested list should not be null.", list);
        assertFalse("The nested list should not be empty.", list.isEmpty());
        assertEquals("The nested list should have one element.", 1, list.size());
        Map<String, Object> inner = (Map<String, Object>) list.get(0);
        assertNotNull("The inner map should not be null.", inner);
        Object testNumber = inner.get(testKey);
        assertNotNull("The entry should not be null.", testNumber);
        assertEquals("The value should be of the expected type.", expectedValue.getClass(), testNumber.getClass());
        assertEquals("The value should be correct.", expectedValue, testNumber);
        convert(actualMap);
    }

}
