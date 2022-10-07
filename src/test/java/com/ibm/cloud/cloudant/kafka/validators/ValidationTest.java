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

import com.ibm.cloud.cloudant.kafka.tasks.SinkConnectorConfig;
import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigValue;
import org.junit.Test;

import java.util.HashMap;

import static com.ibm.cloud.cloudant.kafka.utils.InterfaceConst.APIKEY;
import static com.ibm.cloud.cloudant.kafka.utils.InterfaceConst.AUTH_TYPE;
import static com.ibm.cloud.cloudant.kafka.utils.InterfaceConst.BEARER_TOKEN;
import static com.ibm.cloud.cloudant.kafka.utils.InterfaceConst.DB;
import static com.ibm.cloud.cloudant.kafka.utils.InterfaceConst.IAM_PROFILE_ID;
import static com.ibm.cloud.cloudant.kafka.utils.InterfaceConst.PASSWORD;
import static com.ibm.cloud.cloudant.kafka.utils.InterfaceConst.TOPIC;
import static com.ibm.cloud.cloudant.kafka.utils.InterfaceConst.URL;
import static com.ibm.cloud.cloudant.kafka.utils.InterfaceConst.USERNAME;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ValidationTest {

    // perform all tests against the sink config definition - the source adds a
    // few extra options but they are all booleans so don't have any fancy
    // validation
    private final static ConfigDef CONFIG_DEF = SinkConnectorConfig.CONFIG_DEF;

    @Test
    public void validatesBasicAuth() {
        HashMap<String, String> map = new HashMap<String, String>();
        map.put(AUTH_TYPE, "basic");
        map.put(USERNAME, "test");
        map.put(PASSWORD, "test");
        map.put(URL, "https://somewhere");
        map.put(DB, "animaldb");
        map.put(TOPIC, "foo");
        ConfigValidator validator = new ConfigValidator(
                map,
                CONFIG_DEF);

        Config c = validator.validate();
        assertNoErrorMessages(c);
    }

    @Test
    public void validatesBasicAuthCaseInsensitive() {
        HashMap<String, String> map = new HashMap<String, String>();
        map.put(AUTH_TYPE, "bAsIc");
        map.put(USERNAME, "test");
        map.put(PASSWORD, "test");
        map.put(URL, "https://somewhere");
        map.put(DB, "animaldb");
        map.put(TOPIC, "foo");
        ConfigValidator validator = new ConfigValidator(
                map,
                CONFIG_DEF);

        Config c = validator.validate();
        assertNoErrorMessages(c);
    }

    @Test
    public void validatesBasicAuthNoPassword() {
        HashMap<String, String> map = new HashMap<String, String>();
        map.put(AUTH_TYPE, "basic");
        map.put(USERNAME, "test");
        map.put(URL, "https://somewhere");
        ConfigValidator validator = new ConfigValidator(
                map,
                CONFIG_DEF);

        Config c = validator.validate();
        assertHasErrorMessage(c, AUTH_TYPE,
                "Both 'cloudant.username' and 'cloudant.password' must be set when using 'cloudant.auth.type' of 'basic'");
    }

    @Test
    public void validatesBasicAuthEmptyPassword() {
        HashMap<String, String> map = new HashMap<String, String>();
        map.put(AUTH_TYPE, "basic");
        map.put(USERNAME, "test");
        map.put(PASSWORD, "");
        map.put(URL, "https://somewhere");
        ConfigValidator validator = new ConfigValidator(
                map,
                CONFIG_DEF);

        Config c = validator.validate();
        assertHasErrorMessage(c, AUTH_TYPE,
                "Both 'cloudant.username' and 'cloudant.password' must be set when using 'cloudant.auth.type' of 'basic'");
    }

    @Test
    public void validatesBasicAuthNoUsername() {
        HashMap<String, String> map = new HashMap<String, String>();
        map.put(AUTH_TYPE, "basic");
        map.put(PASSWORD, "test");
        map.put(URL, "https://somewhere");
        ConfigValidator validator = new ConfigValidator(
                map,
                CONFIG_DEF);

        Config c = validator.validate();
        assertHasErrorMessage(c, AUTH_TYPE,
                "Both 'cloudant.username' and 'cloudant.password' must be set when using 'cloudant.auth.type' of 'basic'");
    }

    @Test
    public void validatesBasicAuthEmptyUsername() {
        HashMap<String, String> map = new HashMap<String, String>();
        map.put(AUTH_TYPE, "basic");
        map.put(USERNAME, "");
        map.put(PASSWORD, "test");
        map.put(URL, "https://somewhere");
        ConfigValidator validator = new ConfigValidator(
                map,
                CONFIG_DEF);

        Config c = validator.validate();
        assertHasErrorMessage(c, AUTH_TYPE,
                "Both 'cloudant.username' and 'cloudant.password' must be set when using 'cloudant.auth.type' of 'basic'");
    }

    @Test
    public void validatesUnknownAuth() {
        HashMap<String, String> map = new HashMap<String, String>();
        map.put(AUTH_TYPE, "magic beans");
        map.put(USERNAME, "test");
        map.put(PASSWORD, "test");
        map.put(URL, "https://somewhere");
        ConfigValidator validator = new ConfigValidator(
                map,
                CONFIG_DEF);

        Config c = validator.validate();
        assertHasErrorMessage(c, AUTH_TYPE, "Invalid value magic beans");
        assertHasErrorMessage(c, AUTH_TYPE, "Value must be one of");
    }

    @Test
    public void validatesBadUrl() {
        HashMap<String, String> map = new HashMap<String, String>();
        map.put(AUTH_TYPE, "basic");
        map.put(USERNAME, "test");
        map.put(PASSWORD, "test");
        map.put(URL, "not-a-url");
        ConfigValidator validator = new ConfigValidator(
                map,
                CONFIG_DEF);

        Config c = validator.validate();
        assertHasErrorMessage(c, URL, "Invalid value not-a-url");
        assertHasErrorMessage(c, URL, "Value not a URL");
    }

    @Test
    public void validatesIamAuth() {
        HashMap<String, String> map = new HashMap<String, String>();
        map.put(AUTH_TYPE, "IAM");
        map.put(APIKEY, "test");
        map.put(URL, "https://somewhere");
        map.put(DB, "animaldb");
        map.put(TOPIC, "foo");
        ConfigValidator validator = new ConfigValidator(
                map,
                CONFIG_DEF);

        Config c = validator.validate();
        assertNoErrorMessages(c);
    }

    @Test
    public void validatesIamAuthNoApikey() {
        HashMap<String, String> map = new HashMap<String, String>();
        map.put(AUTH_TYPE, "IAM");
        map.put(URL, "https://somewhere");
        map.put(DB, "animaldb");
        map.put(TOPIC, "foo");
        ConfigValidator validator = new ConfigValidator(
                map,
                CONFIG_DEF);

        Config c = validator.validate();
        assertHasErrorMessage(c, AUTH_TYPE,
                "'cloudant.apikey' must be set when using 'cloudant.auth.type' of 'iam'");
    }

    @Test
    public void validatesSessionAuth() {
        HashMap<String, String> map = new HashMap<String, String>();
        map.put(AUTH_TYPE, "couchdb_session");
        map.put(USERNAME, "test");
        map.put(PASSWORD, "test");
        map.put(URL, "https://somewhere");
        map.put(DB, "animaldb");
        map.put(TOPIC, "foo");
        ConfigValidator validator = new ConfigValidator(
                map,
                CONFIG_DEF);

        Config c = validator.validate();
        assertNoErrorMessages(c);
    }

    @Test
    public void validatesSessionAuthNoCredentials() {
        HashMap<String, String> map = new HashMap<String, String>();
        map.put(AUTH_TYPE, "couchdb_session");
        map.put(URL, "https://somewhere");
        map.put(DB, "animaldb");
        map.put(TOPIC, "foo");
        ConfigValidator validator = new ConfigValidator(
                map,
                CONFIG_DEF);

        Config c = validator.validate();
        assertHasErrorMessage(c, AUTH_TYPE,
                "Both 'cloudant.username' and 'cloudant.password' must be set when using 'cloudant.auth.type' of 'COUCHDB_SESSION'");
    }

    @Test
    public void validatesBearerTokenAuth() {
        HashMap<String, String> map = new HashMap<String, String>();
        map.put(AUTH_TYPE, "bearerToken");
        map.put(BEARER_TOKEN, "test");
        map.put(URL, "https://somewhere");
        map.put(DB, "animaldb");
        map.put(TOPIC, "foo");
        ConfigValidator validator = new ConfigValidator(
                map,
                CONFIG_DEF);

        Config c = validator.validate();
        assertNoErrorMessages(c);
    }

    @Test
    public void validatesBearerTokenAuthNoToken() {
        HashMap<String, String> map = new HashMap<String, String>();
        map.put(AUTH_TYPE, "bearerToken");
        map.put(URL, "https://somewhere");
        map.put(DB, "animaldb");
        map.put(TOPIC, "foo");
        ConfigValidator validator = new ConfigValidator(
                map,
                CONFIG_DEF);

        Config c = validator.validate();
        assertHasErrorMessage(c, AUTH_TYPE,
                "'cloudant.bearer.token' must be set when using 'cloudant.auth.type' of 'bearerToken'");
    }

    @Test
    public void validatesContainerAuth() {
        HashMap<String, String> map = new HashMap<String, String>();
        map.put(AUTH_TYPE, "container");
        map.put(IAM_PROFILE_ID, "test");
        map.put(URL, "https://somewhere");
        map.put(DB, "animaldb");
        map.put(TOPIC, "foo");
        ConfigValidator validator = new ConfigValidator(
                map,
                CONFIG_DEF);

        Config c = validator.validate();
        assertNoErrorMessages(c);
    }

    @Test
    public void validatesContainerAuthNoCredentials() {
        HashMap<String, String> map = new HashMap<String, String>();
        map.put(AUTH_TYPE, "container");
        map.put(URL, "https://somewhere");
        map.put(DB, "animaldb");
        map.put(TOPIC, "foo");
        ConfigValidator validator = new ConfigValidator(
                map,
                CONFIG_DEF);

        Config c = validator.validate();
        assertHasErrorMessage(c, AUTH_TYPE,
                "At least one of 'cloudant.iam.profile.id' or 'cloudant.iam.profile.name' must be set when using 'cloudant.auth.type' of 'container'");
    }

    @Test
    public void validatesVpcAuth() {
        HashMap<String, String> map = new HashMap<String, String>();
        map.put(AUTH_TYPE, "vpc");
        map.put(IAM_PROFILE_ID, "test");
        map.put(URL, "https://somewhere");
        map.put(DB, "animaldb");
        map.put(TOPIC, "foo");
        ConfigValidator validator = new ConfigValidator(
                map,
                CONFIG_DEF);

        Config c = validator.validate();
        assertNoErrorMessages(c);
    }

    @Test
    public void validatesVpcAuthNoCredentials() {
        HashMap<String, String> map = new HashMap<String, String>();
        map.put(AUTH_TYPE, "vpc");
        map.put(URL, "https://somewhere");
        map.put(DB, "animaldb");
        map.put(TOPIC, "foo");
        ConfigValidator validator = new ConfigValidator(
                map,
                CONFIG_DEF);

        Config c = validator.validate();
        assertHasErrorMessage(c, AUTH_TYPE,
                "At least one of 'cloudant.iam.profile.id' or 'cloudant.iam.profile.crn' must be set when using 'cloudant.auth.type' of 'vpc'");
    }

    private static void assertHasErrorMessage(Config config, String property, String msg) {
        for (ConfigValue configValue : config.configValues()) {
            if (configValue.name().equals(property)) {
                assertFalse(configValue.errorMessages().isEmpty());
                assertTrue(configValue.errorMessages().get(0).contains(msg));
            }
        }
    }

    private static void assertNoErrorMessages(Config config) {
        for (ConfigValue configValue : config.configValues()) {
            assertTrue(configValue.errorMessages().isEmpty());
        }
    }

}
