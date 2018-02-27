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
package com.ibm.cloudant.kafka.connect.utils;

import com.cloudant.client.api.ClientBuilder;
import com.cloudant.client.api.CloudantClient;
import com.cloudant.client.api.Database;
import com.cloudant.http.interceptors.Replay429Interceptor;

public class CloudantDbUtils {
	
	public static void dropDatabase(String url, String username, String password) {

		CloudantClient cantClient = null;
		Database cantDB = null; 

		// Create a new CloudantClient instance for account endpoint account.cloudant.com
		// url: https://account.cloudant.com/dbname
		// length of "://" is 3
		if (url != null) {
			String urlWithoutProtocal = url.substring(url.indexOf("://") +3);

			if (urlWithoutProtocal != null) {
				String account = urlWithoutProtocal.substring(0,urlWithoutProtocal.indexOf("."));
				if (account != null) {
					cantClient = ClientBuilder.account(account)
							.username(username)
							.password(password)
							.interceptors(Replay429Interceptor.WITH_DEFAULTS)
							.build();


					String cantDBName = url.substring(url.lastIndexOf("/")+1);

					if (cantDBName != null) {

						cantClient.deleteDB(cantDBName);
					}
				}
			}
		}
	}
}
