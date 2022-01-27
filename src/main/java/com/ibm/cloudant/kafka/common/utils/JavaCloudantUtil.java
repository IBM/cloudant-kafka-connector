/*
 * Copyright © 2016, 2021 IBM Corp. All rights reserved.
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

import com.ibm.cloud.cloudant.security.CouchDbSessionAuthenticator;
import com.ibm.cloud.cloudant.v1.Cloudant;
import com.ibm.cloud.cloudant.v1.model.BulkDocs;
import com.ibm.cloud.cloudant.v1.model.Document;
import com.ibm.cloud.cloudant.v1.model.DocumentResult;
import com.ibm.cloud.cloudant.v1.model.Ok;
import com.ibm.cloud.cloudant.v1.model.PostBulkDocsOptions;
import com.ibm.cloud.cloudant.v1.model.PutDatabaseOptions;
import com.ibm.cloud.sdk.core.http.Response;
import com.ibm.cloud.sdk.core.security.Authenticator;
import com.ibm.cloud.sdk.core.security.NoAuthAuthenticator;
import com.ibm.cloud.sdk.core.service.exception.ServiceResponseException;
import com.ibm.cloudant.kafka.common.CloudantConst;
import com.ibm.cloudant.kafka.common.InterfaceConst;
import com.ibm.cloudant.kafka.common.MessageKey;

import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class JavaCloudantUtil {

	public static final String VERSION;

	private static final String PROPS_FILE = "META-INF/com.ibm.cloudant.kafka.client.properties";
	private static Logger LOG = Logger.getLogger(JavaCloudantUtil.class.toString());

	static {
		String v = "UNKNOWN";
		Properties p = new Properties();
		try (InputStream is = JavaCloudantUtil.class.getClassLoader().getResourceAsStream
			(PROPS_FILE)) {
			if (is != null) {
				p.load(is);
				v = p.getProperty("user.agent.version", "UNKNOWN");
			}
		} catch (IOException e) {
			LOG.warn(PROPS_FILE, e);
		}
		VERSION = v;
	}

	public static JSONArray batchWrite(String url, String userName, String password, JSONArray data)
		throws JSONException {
		// wrap result to JSONArray
		JSONArray result = new JSONArray();
		JSONObject jsonResult = new JSONObject();
		try {
			// get client object
			Cloudant service = getClientInstance(url, userName, password);

			List<Document> listOfDocs = new ArrayList<>();
			for(int i=0; i < data.length(); i++){
				Map<String, Object> item = data.getJSONObject(i).toMap();
				Document doc  = new Document();
				doc.setProperties(item);
				listOfDocs.add(doc);
			}

			// attempt to create database
			String dbName = getDbNameFromUrl(url);
			try {
				PutDatabaseOptions dbOptions = new PutDatabaseOptions.Builder()
					.db(dbName)
					.build();
				service.putDatabase(dbOptions).execute();
			} catch (ServiceResponseException sre) {
				if (sre.getStatusCode() == 412) {
					LOG.info(String.format("Tried to create database %s but it already exists." +
						"Continuing with batch write.", dbName));
				} else {
					sre.printStackTrace();
				}
			}

			//perform bulk insert for array of documents
			BulkDocs docs = new BulkDocs.Builder().docs(listOfDocs).build();
			PostBulkDocsOptions postBulkDocsOptions = new PostBulkDocsOptions.Builder()
				.db(JavaCloudantUtil.getDbNameFromUrl(url))
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

	public static Cloudant getClientInstance(Map<String, String> props)
		throws MalformedURLException {
		return getClientInstance(
			props.get(InterfaceConst.URL),
			props.get(InterfaceConst.USER_NAME),
			props.get(InterfaceConst.PASSWORD));
	}

	public static Cloudant getClientInstance(String url, String username, String
		password) throws MalformedURLException {
		// Create a new CloudantClient instance
		// In future this should be changed so that we don't assume the URL has a path element for
		// the database because some use cases proxy a database to a URL without a path element.
		// dbUrl: https://account.cloudant.com/dbname
		// serverUrl: https://account.cloudant.com/
		Authenticator authenticator = null;
		URL dbUrl = new URL(url);
		URL serverUrl = new URL(dbUrl.getProtocol(), dbUrl.getHost(), dbUrl.getPort(), "");
		if ((username == null || username.isEmpty())
			&& (password == null || password.isEmpty())) {
			authenticator = new NoAuthAuthenticator();
		} else if (username.length() > 0 && password.length() > 0) {
			authenticator = CouchDbSessionAuthenticator.newAuthenticator(username, password);
		} else {
			LOG.error("Username and password should both exist.");
		}

		Cloudant service = new Cloudant(Cloudant.DEFAULT_SERVICE_NAME, authenticator);
		service.setServiceUrl(serverUrl.toString());
		service.enableRetries(3, 1000);
		return service;
	}

	public static String getDbNameFromUrl(Map<String, String> props) {
		return getDbNameFromUrl(props.get(InterfaceConst.URL));
	}

	public static String getDbNameFromUrl(String dbUrl) {
		String dbName = null;
		try {
			dbName = new URL(dbUrl).getPath();
		} catch (MalformedURLException e) {
			e.printStackTrace();
		}
		// A path might have leading or trailing slashes, remove them
		if (dbName.startsWith("/")) {
			dbName = dbName.replaceFirst("/", "");
		}
		if (dbName.endsWith("/")) {
			dbName = dbName.substring(0, dbName.length() - 1);
		}
		return dbName;
	}

	public static void createTargetDb(Cloudant service, String dbName) {
		Response<Ok> result;
		PutDatabaseOptions dbOptions = new PutDatabaseOptions.Builder()
			.db(dbName)
			.build();
		try {
			result = service.putDatabase(dbOptions).execute();
		} catch (ServiceResponseException sre) {
			//sre.printStackTrace();
			// error can happen if db exists
			LOG.error(String.format("Error during creation of database %s.  Error code: %d Error response: %s",
				dbName, sre.getStatusCode(), sre.getMessage()));
		}
	}
}
