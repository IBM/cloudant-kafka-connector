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

package com.ibm.cloudant.kafka.connect;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.ConfigDef.Recommender;
import org.apache.kafka.common.config.ConfigDef.Validator;

// Recommend and Validate that value must be one from a given list
public class ListRecommender implements Recommender, Validator {

    List<Object> validValues;

    public ListRecommender(Object... validValues) {
        this.validValues = Arrays.asList(validValues);
    }

    @Override
    public List<Object> validValues(String name, Map<String, Object> parsedConfig) {
        return validValues;
    }

    @Override
    public boolean visible(String name, Map<String, Object> parsedConfig) {
        return true;
    }

    @Override
    public void ensureValid(String key, Object value) {

        if (value == null) {
            return;
        }

        if (!validValues.contains(value)) {
            throw new ConfigException(key, value, "Value must be one of: " + this.validValues);
        }

    }
}