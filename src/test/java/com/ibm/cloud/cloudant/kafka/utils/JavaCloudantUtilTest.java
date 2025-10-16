/*
 * Copyright © 2025 IBM Corp. All rights reserved.
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
import org.junit.Test;

public class JavaCloudantUtilTest {

    @Test
    public void testUserAgent() throws Exception {
        // Verify that the UA string is correctly formatted
        // Note this uses "unknown" for version because in test environment the jar manifest is not
        // available
        String expectedUserAgent = "cloudant-kafka-connector/unknown (java.version="
                + System.getProperty("java.version") + "; java.vendor="
                + System.getProperty("java.vendor") + "; os.name=" + System.getProperty("os.name")
                + "; os.version=" + System.getProperty("os.version") + "; os.arch="
                + System.getProperty("os.arch") + "; lang=java;)";

        assertEquals(expectedUserAgent, JavaCloudantUtil.VERSION);
    }
}
