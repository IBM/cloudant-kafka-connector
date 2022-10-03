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

package com.ibm.cloud.cloudant.kafka.validators;

import com.ibm.cloud.cloudant.kafka.utils.InterfaceConst;
import com.ibm.cloud.cloudant.kafka.utils.MessageKey;
import com.ibm.cloud.cloudant.kafka.utils.ResourceBundleUtil;
import com.ibm.cloud.cloudant.security.CouchDbSessionAuthenticator;
import com.ibm.cloud.sdk.core.security.Authenticator;
import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.common.config.types.Password;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

// custom validations, for complex validation rules across multiple fields
public class ConfigValidator {

    private Map<String, ConfigValue> values;
    private List<ConfigValue> validations;

    public ConfigValidator(Map<String, String> props, ConfigDef config) {

        validations = config.validate(props);
        values = validations.stream().collect(Collectors.toMap(ConfigValue::name, Function.identity()));
    }

    public Config validate() {
        validateBasicAuth();
        validateIamAuth();
        validateSessionAuth();
        validateBearerTokenAuth();
        validateContainerAuth();
        validateVpcAuth();
        return new Config(validations);
    }

    private void validateBasicAuth() {
        if (Authenticator.AUTHTYPE_BASIC.equalsIgnoreCase((String) values.get(InterfaceConst.AUTH_TYPE).value())) {
            if (nullOrEmpty(values.get(InterfaceConst.USERNAME).value()) || nullOrEmpty(values.get(InterfaceConst.PASSWORD).value())) {
                String messsage = String.format(ResourceBundleUtil.get(MessageKey.VALIDATION_AUTH_BOTH_MUST_BE_SET),
                        InterfaceConst.USERNAME,
                        InterfaceConst.PASSWORD,
                        InterfaceConst.AUTH_TYPE,
                        Authenticator.AUTHTYPE_BASIC);
                addErrorMessage(InterfaceConst.AUTH_TYPE, messsage);
            }
        }
    }

    private void validateIamAuth() {
        if (Authenticator.AUTHTYPE_IAM.equalsIgnoreCase((String) values.get(InterfaceConst.AUTH_TYPE).value())) {
            if (nullOrEmpty(values.get(InterfaceConst.APIKEY).value())) {
                String messsage = String.format(ResourceBundleUtil.get(MessageKey.VALIDATION_AUTH_MUST_BE_SET),
                        InterfaceConst.APIKEY,
                        InterfaceConst.AUTH_TYPE,
                        Authenticator.AUTHTYPE_IAM);
                addErrorMessage(InterfaceConst.AUTH_TYPE, messsage);
            }
        }
    }

    private void validateSessionAuth() {
        if (CouchDbSessionAuthenticator.AUTH_TYPE.equalsIgnoreCase((String) values.get(InterfaceConst.AUTH_TYPE).value())) {
            if (nullOrEmpty(values.get(InterfaceConst.USERNAME).value()) || nullOrEmpty(values.get(InterfaceConst.PASSWORD).value())) {
                String messsage = String.format(ResourceBundleUtil.get(MessageKey.VALIDATION_AUTH_BOTH_MUST_BE_SET),
                        InterfaceConst.USERNAME,
                        InterfaceConst.PASSWORD,
                        InterfaceConst.AUTH_TYPE,
                        CouchDbSessionAuthenticator.AUTH_TYPE);
                addErrorMessage(InterfaceConst.AUTH_TYPE, messsage);
            }
        }
    }

    private void validateBearerTokenAuth() {
        if (Authenticator.AUTHTYPE_BEARER_TOKEN.equalsIgnoreCase((String) values.get(InterfaceConst.AUTH_TYPE).value())) {
            if (nullOrEmpty(values.get(InterfaceConst.BEARER_TOKEN).value())) {
                String messsage = String.format(ResourceBundleUtil.get(MessageKey.VALIDATION_AUTH_MUST_BE_SET),
                        InterfaceConst.BEARER_TOKEN,
                        InterfaceConst.AUTH_TYPE,
                        Authenticator.AUTHTYPE_BEARER_TOKEN);
                addErrorMessage(InterfaceConst.AUTH_TYPE, messsage);
            }
        }
    }

    private void validateContainerAuth() {
        if (Authenticator.AUTHTYPE_CONTAINER.equalsIgnoreCase((String) values.get(InterfaceConst.AUTH_TYPE).value())) {
            if (nullOrEmpty(values.get(InterfaceConst.IAM_PROFILE_ID).value()) && nullOrEmpty(values.get(InterfaceConst.IAM_PROFILE_NAME).value())) {
                String messsage = String.format(ResourceBundleUtil.get(MessageKey.VALIDATION_AUTH_AT_LEAST_ONE_MUST_BE_SET),
                        InterfaceConst.IAM_PROFILE_ID,
                        InterfaceConst.IAM_PROFILE_NAME,
                        InterfaceConst.AUTH_TYPE,
                        Authenticator.AUTHTYPE_CONTAINER);
                addErrorMessage(InterfaceConst.AUTH_TYPE, messsage);
            }
        }
    }

    private void validateVpcAuth() {
        if (Authenticator.AUTHTYPE_VPC.equalsIgnoreCase((String) values.get(InterfaceConst.AUTH_TYPE).value())) {
            if (nullOrEmpty(values.get(InterfaceConst.IAM_PROFILE_ID).value()) && nullOrEmpty(values.get(InterfaceConst.IAM_PROFILE_CRN).value())) {
                String messsage = String.format(ResourceBundleUtil.get(MessageKey.VALIDATION_AUTH_AT_LEAST_ONE_MUST_BE_SET),
                        InterfaceConst.IAM_PROFILE_ID,
                        InterfaceConst.IAM_PROFILE_CRN,
                        InterfaceConst.AUTH_TYPE,
                        Authenticator.AUTHTYPE_VPC);
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
            if (((String) str).isEmpty()) {
                return true;
            }
        }
        if (str instanceof Password) {
            if (((Password) str).value().isEmpty()) {
                return true;
            }
        }
        return false;
    }
}
