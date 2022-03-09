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

package com.ibm.cloudant.kafka;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Map;

import com.ibm.cloudant.kafka.connect.CloudantSinkConnectorConfig;
import com.ibm.cloudant.kafka.connect.CustomValidator;

import static com.ibm.cloudant.kafka.common.InterfaceConst.AUTH_TYPE;
import static com.ibm.cloudant.kafka.common.InterfaceConst.USER_NAME;
import static com.ibm.cloudant.kafka.common.InterfaceConst.PASSWORD;
import static com.ibm.cloudant.kafka.common.InterfaceConst.URL;
import static com.ibm.cloudant.kafka.common.InterfaceConst.TOPIC;

import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigValue;
import org.junit.Test;

public class ValidationTest {

    // perform all tests against the sink config definition - the source adds a
    // few extra options but they are all booleans so don't have any fancy
    // validation
    private final static ConfigDef CONFIG_DEF = CloudantSinkConnectorConfig.CONFIG_DEF;

    @Test
    public void validatesBasicAuth() {
        CustomValidator validator = new CustomValidator(
                Map.of(AUTH_TYPE, "basic",
                        USER_NAME, "test",
                        PASSWORD, "test",
                        URL, "https://somewhere",
                        TOPIC, "foo"),
                CONFIG_DEF);

        Config c = validator.validate();
        assertNoErrorMessages(c);
    }

    @Test
    public void validatesBasicAuthNoPassword() {
        CustomValidator validator = new CustomValidator(
                Map.of(AUTH_TYPE, "basic",
                        USER_NAME, "test",
                        URL, "https://somewhere"),
                CONFIG_DEF);

        Config c = validator.validate();
        assertHasErrorMessage(c, AUTH_TYPE,
                "Both 'cloudant.username' and 'cloudant.password' must be set when using 'cloudant.auth.type' of 'basic'");
    }

    @Test
    public void validatesBasicAuthEmptyPassword() {
        CustomValidator validator = new CustomValidator(
                Map.of(AUTH_TYPE, "basic",
                        USER_NAME, "test",
                        PASSWORD, "",
                        URL, "https://somewhere"),
                CONFIG_DEF);

        Config c = validator.validate();
        assertHasErrorMessage(c, AUTH_TYPE,
                "Both 'cloudant.username' and 'cloudant.password' must be set when using 'cloudant.auth.type' of 'basic'");
    }

    @Test
    public void validatesBasicAuthNoUsername() {
        CustomValidator validator = new CustomValidator(
                Map.of(AUTH_TYPE, "basic",
                        PASSWORD, "test",
                        URL, "https://somewhere"),
                CONFIG_DEF);

        Config c = validator.validate();
        assertHasErrorMessage(c, AUTH_TYPE,
                "Both 'cloudant.username' and 'cloudant.password' must be set when using 'cloudant.auth.type' of 'basic'");
    }

    @Test
    public void validatesBasicAuthEmptyUsername() {
        CustomValidator validator = new CustomValidator(
                Map.of(AUTH_TYPE, "basic",
                        USER_NAME, "",
                        PASSWORD, "test",
                        URL, "https://somewhere"),
                CONFIG_DEF);

        Config c = validator.validate();
        assertHasErrorMessage(c, AUTH_TYPE,
                "Both 'cloudant.username' and 'cloudant.password' must be set when using 'cloudant.auth.type' of 'basic'");
    }

    @Test
    public void validatesUnknownAuth() {
        CustomValidator validator = new CustomValidator(
                Map.of(AUTH_TYPE, "magic beans",
                        USER_NAME, "test",
                        PASSWORD, "test",
                        URL, "https://somewhere"),
                CONFIG_DEF);

        Config c = validator.validate();
        assertHasErrorMessage(c, AUTH_TYPE, "Invalid value magic beans");
    }

    @Test
    public void validatesBadUrl() {
        CustomValidator validator = new CustomValidator(
                Map.of(AUTH_TYPE, "basic",
                        USER_NAME, "test",
                        PASSWORD, "test",
                        URL, "not-a-url"),
                CONFIG_DEF);

        Config c = validator.validate();
        assertHasErrorMessage(c, URL, "Invalid value not-a-url");
    }

    private static void assertHasErrorMessage(Config config, String property, String msg) {
        for (ConfigValue configValue : config.configValues()) {
            if (configValue.name().equals(property)) {
                assertFalse(configValue.errorMessages().isEmpty());
                assertTrue(configValue.errorMessages().get(0).contains(msg));
                System.out.println(configValue.errorMessages().get(0));
            }
        }
    }

    private static void assertNoErrorMessages(Config config) {
        for (ConfigValue configValue : config.configValues()) {
            assertTrue(configValue.errorMessages().isEmpty());
        }
    }

}
