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
package com.ibm.cloud.cloudant.kafka.utils;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.UnaryOperator;

/**
 * An extension of HashMap that converts Number type entries being added to the Map.
 * It recurses into Collections being added to ensure their Number values are also converted.
 * 
 * The principle is that the string value of a number is converted to a BigDecimal that can
 * hold arbitrary precision values. Then it attempts to narrow down to the most specific Number
 * type that can hold the value. This means that any use in a schema later can easily widen the
 * value without losing precision, whereas returning a wider type might cause narrow schema
 * types to fail.
 * 
 * Values without float content are preferentially converted to whole number types because the max
 * of Integer (2^32 -1) exceeds that of the set of integers fully representable in Float (2^24)
 * and Long (2^64 -1) exceeds that of Double (2^53). This makes it worth noting that
 * Float.MAX_VALUE or Double.MAX_VALUE will be converted to BigInteger
 * (since the value exceeds that representable by Long), but those floating point max values
 * cannot represent all integer values lower than them.
 */
public class NumberSafeMap extends HashMap<String, Object> {

    private static final NumberMapper mapper = new NumberMapper();

    public NumberSafeMap() {
        super();
    }

    public NumberSafeMap(int length) {
        super(length);
    }

    public NumberSafeMap(Map<String, Object> map) {
        this(map.size());
        this.putAll(map);
    }

    @Override
    public Object put(String key, Object value) {
        return super.put(key, value != null ? mapper.apply(value): value);
    }

    @Override
    public void putAll(Map<?  extends String, ? extends Object> m) {
        m.forEach((k, v) -> this.put(k, v));
    }

    @SuppressWarnings("unchecked")
    private static final class NumberMapper implements UnaryOperator<Object> {

        // A list of the preference order by which we try to narrow the value into a specific Number type.
        private static final List<Function<BigDecimal, ? extends Number>> narrowingFunctions = Collections.unmodifiableList(
            Arrays.asList(
                BigDecimal::byteValueExact,
                BigDecimal::shortValueExact,
                BigDecimal::intValueExact,
                BigDecimal::longValueExact,
                BigDecimal::toBigIntegerExact,
                (n) -> {
                    float f = n.floatValue();
                    if (f != Float.NEGATIVE_INFINITY && f != Float.POSITIVE_INFINITY && n.equals(new BigDecimal(f))) {
                        // Exactly representable by float
                        return f;
                    } else {
                        throw new ArithmeticException();
                    }
                },
                (n) -> n.doubleValue() // Last resort is a double value, noting that CouchDB/Cloudant JSON source numbers are limited to IEEE754 doubles
                ));

        @Override
        public Object apply(Object value) {
            // The underlying client SDK uses GSON's LazilyParsedNumber.
            // These are incompatible with the Kafka JsonConverter so we
            // need to change them to a built-in Number type.
            // Leave BigInteger and BigDecimal as is
            if (value instanceof Number && !(value instanceof BigInteger || value instanceof BigDecimal)) {
                // Make a BigDecimal from the Number as it can hold the values of all other types
                BigDecimal number = new BigDecimal(value.toString());
                for (Function<BigDecimal, ? extends Number> narrowingFunction : narrowingFunctions) {
                    try{
                        return narrowingFunction.apply(number);
                    } catch(ArithmeticException n) {
                        // The narrowing function was not suitable, try the next one.
                        continue;
                    }
                }
                // The last resort narrowing function should always return a double so this code should be unreachable.
                // In case the list of narrowing functions changes without updating this code, fallback
                // to returning the BigDecimal value of arbitrary precision.
                return number;
            } else if (value instanceof Map) {
                return new NumberSafeMap((Map<String, Object>) value);
            } else if (value instanceof List) {
                // Make a new list instead of doing direct replacement
                // just in case the original is unmodifiable.
                List<Object> numberSafeList = new ArrayList<>(((List<Object>) value));
                numberSafeList.replaceAll(mapper);
                return numberSafeList;
            } else {
                return value;
            }
        }
    }
}
