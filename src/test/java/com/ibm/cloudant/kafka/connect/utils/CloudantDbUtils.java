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

import com.cloudant.client.api.CloudantClient;
import com.ibm.cloudant.kafka.common.utils.JavaCloudantUtil;

import java.net.MalformedURLException;

public class CloudantDbUtils {

	public static void dropDatabase(String url, String username, String password) throws
			MalformedURLException {
		CloudantClient client = JavaCloudantUtil.getClientInstanceFromDBUrl(url, username,
				password);
		String dbName = JavaCloudantUtil.getDbNameFromUrl(url);
		if (dbName != null) {
			client.deleteDB(dbName);
		}
	}
}
