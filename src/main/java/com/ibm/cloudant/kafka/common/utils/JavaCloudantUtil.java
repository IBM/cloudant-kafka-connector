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
package com.ibm.cloudant.kafka.common.utils;

import com.ibm.cloud.cloudant.internal.ServiceFactory;
import com.ibm.cloud.cloudant.v1.Cloudant;
import com.ibm.cloud.cloudant.v1.model.BulkDocs;
import com.ibm.cloud.cloudant.v1.model.Document;
import com.ibm.cloud.cloudant.v1.model.DocumentResult;
import com.ibm.cloud.cloudant.v1.model.PostBulkDocsOptions;
import com.ibm.cloud.cloudant.v1.model.PutDatabaseOptions;
import com.ibm.cloud.sdk.core.service.exception.ServiceResponseException;
import com.ibm.cloudant.kafka.common.CloudantConst;
import com.ibm.cloudant.kafka.common.InterfaceConst;
import com.ibm.cloudant.kafka.common.MessageKey;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;

public class JavaCloudantUtil {

	public static final String VERSION;

	private static final String PROPS_FILE = "META-INF/com.ibm.cloudant.kafka.client.properties";
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
			p.getProperty("user.agent.name", "kafka-connect-cloudant"),
			p.getProperty("user.agent.version", "UNKNOWN"),
			System.getProperty("java.version", "UNKNOWN"),
			System.getProperty("java.vendor", "UNKNOWN"),
			System.getProperty("os.name", "UNKNOWN"),
			System.getProperty("os.arch", "UNKNOWN")
		);
	}

	public static JSONArray batchWrite(Map<String, String> props, JSONArray data)
		throws JSONException {
		// wrap result to JSONArray
		JSONArray result = new JSONArray();
		JSONObject jsonResult = new JSONObject();
		try {
			// get client object
			Cloudant service = getClientInstance(props);

			List<Document> listOfDocs = new ArrayList<>();
			for(int i=0; i < data.length(); i++){
				Map<String, Object> docProperties = data.getJSONObject(i).toMap();
				Document doc = new Document();
				doc.setProperties(docProperties);
				listOfDocs.add(doc);
			}

			// attempt to create database
			createTargetDb(service, props.get(InterfaceConst.DB));

			//perform bulk insert for array of documents
			BulkDocs docs = new BulkDocs.Builder().docs(listOfDocs).build();
			PostBulkDocsOptions postBulkDocsOptions = new PostBulkDocsOptions.Builder()
				.db(props.get(InterfaceConst.DB))
				.bulkDocs(docs)
				.build();
			List<DocumentResult> resList = service.postBulkDocs(postBulkDocsOptions).execute().getResult();

			for(int j=0; j < resList.size();j++){
				DocumentResult documentResult = resList.get(j);

				// construct response which is similar to doPost()
				// {"rev":"380-270e81b096fe9ed54dc42a14b47467b9","id":"kafka@database","ok":true}
				jsonResult.put(CloudantConst.RESPONSE_ID,documentResult.getId());
				jsonResult.put(CloudantConst.RESPONSE_REV,documentResult.getRev());
				jsonResult.put(CloudantConst.RESPONSE_ERROR,documentResult.getError());
				jsonResult.put(CloudantConst.RESPONSE_REASON,documentResult.getReason());
				if (documentResult.getError() != null) {
					jsonResult.put(CloudantConst.RESPONSE_OK,false);
					// TODO support status code field in documentresult schema?
					jsonResult.put(CloudantConst.RESPONSE_CODE, 400);
				} else {
					jsonResult.put(CloudantConst.RESPONSE_OK,true);
					jsonResult.put(CloudantConst.RESPONSE_CODE, 201);
				}

				result.put(jsonResult);
			}
		} catch (Exception e) {
			if(e.getMessage().equals(String.format(ResourceBundleUtil.get(
				MessageKey.CLOUDANT_LIMITATION)))){
				// try to put items from jsonResult before exception occurred
				result.put(jsonResult);
			}
		}
		return result;
	}

	public static Cloudant getClientInstance(Map<String, String> props) {
		return ServiceFactory.getInstance(props, VERSION);
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
				LOG.info(String.format("Tried to create database %s but it already exists.", dbName));
			} else {
				LOG.error(String.format("Error during creation of database %s.  Error code: %d Error response: %s",
					dbName, sre.getStatusCode(), sre.getMessage()));
				sre.printStackTrace();
			}
		}
	}
}
