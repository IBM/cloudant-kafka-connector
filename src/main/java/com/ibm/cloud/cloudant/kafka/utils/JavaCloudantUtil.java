/*
 * Copyright © 2016, 2023 IBM Corp. All rights reserved.
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

import com.ibm.cloud.cloudant.v1.Cloudant;
import com.ibm.cloud.cloudant.v1.model.*;
import com.ibm.cloud.sdk.core.service.exception.ServiceResponseException;
import com.ibm.cloud.cloudant.kafka.caching.CachedClientManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;

public class JavaCloudantUtil {

    public static final String VERSION;

    private static final String PROPS_FILE = "META-INF/com.ibm.cloud.cloudant.kafka.client.properties";
    private static Logger LOG = LoggerFactory.getLogger(JavaCloudantUtil.class.toString());

    static {
        Properties p = new Properties();
        try (InputStream is = JavaCloudantUtil.class.getClassLoader().getResourceAsStream
                (PROPS_FILE)) {
            if (is != null) {
                p.load(is);
            }
        } catch (IOException e) {
            LOG.warn(PROPS_FILE, e);
        }
        VERSION = String.format(Locale.ENGLISH, "%s/%s/%s/%s/%s/%s",
                p.getProperty("user.agent.name", "cloudant-kafka-connector"),
                p.getProperty("user.agent.version", "UNKNOWN"),
                System.getProperty("java.version", "UNKNOWN"),
                System.getProperty("java.vendor", "UNKNOWN"),
                System.getProperty("os.name", "UNKNOWN"),
                System.getProperty("os.arch", "UNKNOWN")
        );
    }

	public static List<DocumentResult> batchWrite(Map<String, String> props, List<Document> listOfDocs)
		throws RuntimeException {
		Cloudant service = CachedClientManager.getInstance(props);

		// attempt to create database
		createTargetDb(service, props.get(InterfaceConst.DB));

		// perform bulk insert for array of documents
		BulkDocs docs = new BulkDocs.Builder().docs(listOfDocs).build();
		PostBulkDocsOptions postBulkDocsOptions = new PostBulkDocsOptions.Builder()
			.db(props.get(InterfaceConst.DB))
			.bulkDocs(docs)
			.build();

		// caller's responsibility to catch RuntimeException on execute() if thrown
		List<DocumentResult> resList = service.postBulkDocs(postBulkDocsOptions).execute().getResult();
		return resList;
	}

    public static void createTargetDb(Cloudant service, String dbName) {
        PutDatabaseOptions dbOptions = new PutDatabaseOptions.Builder()
                .db(dbName)
                .build();
        try {
            service.putDatabase(dbOptions).execute();
        } catch (ServiceResponseException sre) {
            // error can happen if db exists
            // pass in error message e.g. "Error during creation of database <dbname>"
            if (sre.getStatusCode() == 412) {
                LOG.info("Tried to create database {} but it already exists.", dbName);
            } else {
                LOG.error("Error during creation of database {}.  Error code: {} Error response: {}",
                        dbName, sre.getStatusCode(), sre.getMessage());
                throw sre;
            }
        }
    }
}
