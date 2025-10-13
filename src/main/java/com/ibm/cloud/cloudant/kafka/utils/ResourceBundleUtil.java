/*
 * Copyright © 2016, 2022 IBM Corp. All rights reserved.
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Enumeration;
import java.util.Locale;
import java.util.ResourceBundle;

public class ResourceBundleUtil {

    private static Logger LOG = LoggerFactory.getLogger(ResourceBundleUtil.class);

    private static ResourceBundle rb = null;

    public static ResourceBundle getRb() {

        if (rb == null) {
            Locale locale = Locale.US;
            rb = ResourceBundle.getBundle("message", locale);
        }

        Enumeration<String> keys = rb.getKeys();
        while (keys.hasMoreElements()) {
            String key = keys.nextElement();
            LOG.debug(key);
        }

        return rb;
    }

    public static String get(String key) {
        return getRb().getString(key);
    }
}
