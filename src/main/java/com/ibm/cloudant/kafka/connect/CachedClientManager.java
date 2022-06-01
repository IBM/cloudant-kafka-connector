package com.ibm.cloudant.kafka.connect;

import com.ibm.cloud.cloudant.internal.ServiceFactory;
import com.ibm.cloud.cloudant.v1.Cloudant;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.ibm.cloudant.kafka.common.utils.JavaCloudantUtil.VERSION;

public class CachedClientManager {

    // Cloudant clients, keyed by connector name
    static Map<String, Cloudant> clientCache = new ConcurrentHashMap<>();

    public static Cloudant getInstance(Map<String, String> props) {
        String connectorName = props.get("name");
        return clientCache.computeIfAbsent(connectorName, p -> ServiceFactory.getInstance(props, VERSION));
    }

}
