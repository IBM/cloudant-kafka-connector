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

 package com.ibm.cloud.cloudant.internal;

import java.util.Collections;
import java.util.Map;

import com.ibm.cloud.cloudant.v1.Cloudant;
import com.ibm.cloud.sdk.core.security.Authenticator;
import com.ibm.cloudant.kafka.common.InterfaceConst;

public class ServiceFactory {

    private ServiceFactory() {

    }

    public static Cloudant getInstance(Map<String, String> props, String version) {
        Authenticator authenticator = DelegatingAuthenticatorFactory.getAuthenticator(props);
        Cloudant service = new Cloudant(Cloudant.DEFAULT_SERVICE_NAME, authenticator);
        service.setServiceUrl(props.get(InterfaceConst.URL));
        service.enableRetries(3, 1000);
        service.setDefaultHeaders(Collections.singletonMap("User-Agent", version));
        return service;
    }
}

