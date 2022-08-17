package com.ibm.cloud.cloudant.kafka.connect;

import com.ibm.cloud.cloudant.v1.Cloudant;

public class ClientManagerUtils {

    public static void addClientToCache(String name, Cloudant cloudant) {
        CachedClientManager.clientCache.put(name, cloudant);
    }
}
