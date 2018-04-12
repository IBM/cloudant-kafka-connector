/*
 * Copyright © 2016, 2018 IBM Corp. All rights reserved.
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

import com.cloudant.client.api.ClientBuilder;
import com.cloudant.client.api.CloudantClient;
import com.cloudant.client.api.Database;
import com.cloudant.client.api.model.Response;
import com.cloudant.http.interceptors.Replay429Interceptor;
import com.cloudant.http.internal.interceptors.UserAgentInterceptor;
import com.ibm.cloudant.kafka.common.CloudantConst;
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
import java.util.Properties;

public class JavaCloudantUtil {

	public static final String VERSION;

	private static final String PROPS_FILE = "META-INF/com.ibm.cloudant.kafka.client.properties";
	private static final UserAgentInterceptor UA_INTERCEPTOR = new UserAgentInterceptor
			(JavaCloudantUtil.class.getClassLoader(), PROPS_FILE);
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

	public static JSONArray batchWrite(String url, String userName, String password, JSONArray data) throws JSONException {
		// wrap result to JSONArray
		JSONArray result = new JSONArray();
		
		try {
			// get database object
			Database cantDB = getDBInst(url, userName, password);

			List<Object> entryObj = new ArrayList<Object>();
			for(int i=0; i < data.length();i++){											
				entryObj.add(data.getJSONObject(i).toMap());
			}

			//perform bulk insert for array of documents
			List<Response> resList = cantDB.bulk(entryObj);
		
			for(int j=0; j < resList.size();j++){
				Response res = resList.get(j);
	    	 
				JSONObject jsonResult = new JSONObject();
				// construct response which is similar to doPost()
				// {"rev":"380-270e81b096fe9ed54dc42a14b47467b9","id":"kafka@database","ok":true}
				jsonResult.put(CloudantConst.RESPONSE_ID,res.getId());
				jsonResult.put(CloudantConst.RESPONSE_REV,res.getRev());
				jsonResult.put(CloudantConst.RESPONSE_ERROR,res.getError());
				jsonResult.put(CloudantConst.RESPONSE_REASON,res.getReason());
				if (res.getError() != null) {
					jsonResult.put(CloudantConst.RESPONSE_OK,false);
				} else {
					jsonResult.put(CloudantConst.RESPONSE_OK,true);
				}
				jsonResult.put(CloudantConst.RESPONSE_CODE,res.getStatusCode());
	    	 
				result.put(jsonResult);
			}
		} catch (Exception e) {
			if(e.getMessage().equals(String.format(ResourceBundleUtil.get(
					MessageKey.CLOUDANT_LIMITATION)))){
				// get database object
				Database cantDB = getDBInst(url, userName, password);
							
				List<Object> entryObj = new ArrayList<Object>();
				JSONArray tempData = new JSONArray(data.toString());			
				
				while(tempData.length() > 0) {
					for(int i=0; i < 10;i++){											
						if(tempData.length()>0){
							entryObj.add(tempData.getJSONObject(0).toMap());
							tempData.remove(0);
						}					
					}
					
					//perform bulk insert for array of documents
					List<Response> resList = cantDB.bulk(entryObj);
					entryObj.clear();
				
					for(int j=0; j < resList.size();j++){
						Response res = resList.get(j);
			    	 
						JSONObject jsonResult = new JSONObject();
						// construct response which is similar to doPost()
						// {"rev":"380-270e81b096fe9ed54dc42a14b47467b9","id":"kafka@database","ok":true}
						jsonResult.put(CloudantConst.RESPONSE_ID,res.getId());
						jsonResult.put(CloudantConst.RESPONSE_REV,res.getRev());
						jsonResult.put(CloudantConst.RESPONSE_ERROR,res.getError());
						jsonResult.put(CloudantConst.RESPONSE_REASON,res.getReason());
						if (res.getError() != null) {
							jsonResult.put(CloudantConst.RESPONSE_OK,false);
						} else {
							jsonResult.put(CloudantConst.RESPONSE_OK,true);
						}
						jsonResult.put(CloudantConst.RESPONSE_CODE,res.getStatusCode());
			    	 
						result.put(jsonResult);
					}
				}
			}		
		}

	    return result;
	}

	public static CloudantClient getClientInstanceFromDBUrl(String url, String username, String
			password) throws MalformedURLException {
		// Create a new CloudantClient instance for account endpoint account.cloudant.com
		// In future this should be changed so that we don't assume the URL has a path element for
		// the database because some use cases proxy a database to a URL without a path element.
		// dbUrl: https://account.cloudant.com/dbname
		// serverUrl: https://account.cloudant.com/
		URL dbUrl = new URL(url);
		URL serverUrl = new URL(dbUrl.getProtocol(), dbUrl.getHost(), dbUrl.getPort(), "");
		return ClientBuilder.url(serverUrl)
				.username(username)
				.password(password)
				.interceptors(UA_INTERCEPTOR, Replay429Interceptor.WITH_DEFAULTS)
				.build();
	}

	public static Database getDBInst(String url, String username, String password) {
		return getDBInst(url, username, password, true);
	}

	public static Database getDBInst(String url, String username, String password, boolean
			create) {
		RuntimeException error = new RuntimeException(String.format(ResourceBundleUtil.get
				(MessageKey.CLOUDANT_DATABASE_ERROR), url));
		try {
			CloudantClient cloudantClient = getClientInstanceFromDBUrl(url, username, password);
			String cloudantDbName = getDbNameFromUrl(url);
			if (cloudantDbName != null) {
				// create a database instance (create it if database doesn't exist)
				return cloudantClient.database(cloudantDbName, create);
			} else {
				throw error;
			}
		} catch (MalformedURLException e) {
			throw error;
		}
	}

	public static String getDbNameFromUrl(String dbUrl) throws MalformedURLException {
		String dbName = new URL(dbUrl).getPath();
		// A path might have leading or trailing slashes, remove them
		if (dbName.startsWith("/")) {
			dbName = dbName.replaceFirst("/", "");
		}
		if (dbName.endsWith("/")) {
			dbName = dbName.substring(0, dbName.length() - 1);
		}
		return dbName;
	}
}
