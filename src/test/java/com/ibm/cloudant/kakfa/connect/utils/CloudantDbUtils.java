package com.ibm.cloudant.kakfa.connect.utils;

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
