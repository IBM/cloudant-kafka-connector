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

import com.ibm.cloudant.kafka.common.InterfaceConst;

import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Locale;
import java.util.Map;
import java.util.ResourceBundle;

public class ResourceBundleUtil {
	
	private static Logger LOG = Logger.getLogger(ResourceBundleUtil.class);

	private static ResourceBundle rb = null;
	
	public static ResourceBundle getRb() {
		
		if(rb == null){
			Locale locale = new Locale("en", "US"); 
			rb = ResourceBundle.getBundle("message", locale);
		}
		
		Enumeration<String> keys = rb.getKeys();
		while(keys.hasMoreElements()){
			String key = (String)  keys.nextElement();
			LOG.debug(key);
		}

		return rb;
	}

	public static String get(String key){
		return getRb().getString(key);
	}
	
	public static int numberOfTopics(Map<String, String> configProperties) {
		return configProperties.get(InterfaceConst.TOPIC).split(",").length;
	}
	
	/***
	 * Generate topics to be consumed considering number of 
	 * tasks
	 * @param configProperties the configuration properties from which to extract the list of topics
	 * @param maxTasks max number of tasks
	 * @return a list of topics
	 */
	public static ArrayList<String> balanceTopicsTasks(Map<String, String> configProperties, int maxTasks) {
		// Get topics
		String[] topics = configProperties.get(InterfaceConst.TOPIC).split(",");
		
		int topicsLength = numberOfTopics(configProperties);  
		ArrayList<String> topicsInTask = new ArrayList<String>();
		for (int n=0; n < topicsLength; n++){
			if (n < maxTasks) {
				topicsInTask.add(topics[n]);
			} else {
				topicsInTask.set(n % maxTasks, topicsInTask.get(n % maxTasks) + "," + topics[n]);
			}
		}	
		return topicsInTask;
	}

}
