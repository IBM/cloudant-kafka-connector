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

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.ibm.cloudant.kafka.common.InterfaceConst;

import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.common.config.types.Password;

// custom validations, for complex validation rules across multiple fields
public class CloudantConfigValidator {

    private Map<String, ConfigValue> values;
    private List<ConfigValue> validations;

    public CloudantConfigValidator(Map<String, String> props, ConfigDef config) {

        validations = config.validate(props);
        values = validations.stream().collect(Collectors.toMap(ConfigValue::name, Function.identity()));
    }

    public Config validate() {
        validateBasicAuth();
        return new Config(validations);
    }

    private void validateBasicAuth() {
        String basic = "basic";
        if (values.get(InterfaceConst.AUTH_TYPE).value().equals(basic)) {
            if (nullOrEmpty(values.get(InterfaceConst.USER_NAME).value()) || nullOrEmpty(values.get(InterfaceConst.PASSWORD).value())) {
                String messsage = String.format("Both '%s' and '%s' must be set when using '%s' of '%s'",
                InterfaceConst.USER_NAME,
                InterfaceConst.PASSWORD,
                InterfaceConst.AUTH_TYPE,
                basic);
                addErrorMessage(InterfaceConst.AUTH_TYPE, messsage);
            }
        }
    }

    private void addErrorMessage(String property, String error) {
        values.get(property).addErrorMessage(error);
    }

    private static boolean nullOrEmpty(Object str) {
        if (str == null) {
            return true;
        }
        if (str instanceof String) {
            if (((String)str).isEmpty()) {
                return true;
            }
        }
        if (str instanceof Password) {
            if (((Password)str).value().isEmpty()) {
                return true;
            }
        }
        return false;
    }
}
