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

