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

import com.ibm.cloud.cloudant.security.CouchDbSessionAuthenticator;
import com.ibm.cloud.sdk.core.security.Authenticator;
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
        validateIamAuth();
        validateSessionAuth();
        return new Config(validations);
    }

    private void validateBasicAuth() {
        if (Authenticator.AUTHTYPE_BASIC.equalsIgnoreCase((String)values.get(InterfaceConst.AUTH_TYPE).value())) {
            if (nullOrEmpty(values.get(InterfaceConst.USERNAME).value()) || nullOrEmpty(values.get(InterfaceConst.PASSWORD).value())) {
                String messsage = String.format("Both '%s' and '%s' must be set when using '%s' of '%s'",
                InterfaceConst.USERNAME,
                InterfaceConst.PASSWORD,
                InterfaceConst.AUTH_TYPE,
                Authenticator.AUTHTYPE_BASIC);
                addErrorMessage(InterfaceConst.AUTH_TYPE, messsage);
            }
        }
    }

    private void validateIamAuth() {
        if (Authenticator.AUTHTYPE_IAM.equalsIgnoreCase((String)values.get(InterfaceConst.AUTH_TYPE).value())) {
            if (nullOrEmpty(values.get(InterfaceConst.APIKEY).value())) {
                String messsage = String.format("'%s' must be set when using '%s' of '%s'",
                InterfaceConst.APIKEY,
                InterfaceConst.AUTH_TYPE,
                Authenticator.AUTHTYPE_IAM);
                addErrorMessage(InterfaceConst.AUTH_TYPE, messsage);
            }
        }
    }

    private void validateSessionAuth() {
        if (CouchDbSessionAuthenticator.AUTH_TYPE.equalsIgnoreCase((String)values.get(InterfaceConst.AUTH_TYPE).value())) {
            if (nullOrEmpty(values.get(InterfaceConst.USERNAME).value()) || nullOrEmpty(values.get(InterfaceConst.PASSWORD).value())) {
                String messsage = String.format("Both '%s' and '%s' must be set when using '%s' of '%s'",
                InterfaceConst.USERNAME,
                InterfaceConst.PASSWORD,
                InterfaceConst.AUTH_TYPE,
                CouchDbSessionAuthenticator.AUTH_TYPE);
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
