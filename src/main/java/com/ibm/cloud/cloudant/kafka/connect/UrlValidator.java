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

package com.ibm.cloud.cloudant.kafka.connect;

import com.ibm.cloud.cloudant.kafka.common.MessageKey;
import com.ibm.cloud.cloudant.kafka.common.utils.ResourceBundleUtil;
import org.apache.kafka.common.config.ConfigDef.Validator;
import org.apache.kafka.common.config.ConfigException;

import java.net.MalformedURLException;
import java.net.URL;

public class UrlValidator implements Validator {

    @Override
    public void ensureValid(String name, Object value) {
        if (value instanceof URL) {
            return;
        }
        if (value instanceof String) {
            String stringValue = (String) value;
            try {
                new URL(stringValue);
            } catch (MalformedURLException mue) {
                throw new ConfigException(name, value, String.format(ResourceBundleUtil.get(MessageKey.VALIDATION_NOT_A_URL), value));
            }
            return;
        }
        throw new ConfigException(name, value, String.format(ResourceBundleUtil.get(MessageKey.VALIDATION_NOT_A_URL), value));
    }
}
