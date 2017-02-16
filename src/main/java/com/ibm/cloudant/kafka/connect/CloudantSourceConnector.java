/*******************************************************************************
* Copyright (c) 2016 IBM Corp.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*******************************************************************************/
package com.ibm.cloudant.kafka.connect;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.log4j.Logger;

import com.ibm.cloudant.kafka.common.InterfaceConst;
import com.ibm.cloudant.kafka.common.MessageKey;
import com.ibm.cloudant.kafka.common.utils.ResourceBundleUtil;

public class CloudantSourceConnector extends SourceConnector {
	
	private static Logger LOG = Logger.getLogger(CloudantSourceConnector.class);
	
	private Map<String, String> configProperties;
	private CloudantSourceConnectorConfig config;

	@Override
	public ConfigDef config() {
		return CloudantSourceConnectorConfig.CONFIG_DEF;
	}

	@Override
	public void start(Map<String, String> props) {
		
	     configProperties = props;
		 config = new CloudantSourceConnectorConfig(configProperties);	
	}

	@Override
	public void stop() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Class<? extends Task> taskClass() {
		return CloudantSourceTask.class;
	}

	
	@Override
	public List<Map<String, String>> taskConfigs(int maxTasks) {
		
		try {
			

			ArrayList<String> topics = balanceTopicsTasks(maxTasks);
			int topicsLength = numberOfTopics();
			List<Map<String, String>> taskConfigs = new ArrayList<Map<String, String>>(maxTasks);
			
			for (int i = 0; i < maxTasks; i++) {
				Map<String, String> taskProps = new HashMap<String, String>(configProperties);
			
				// add task specific properties here (if any)
				taskProps.put(InterfaceConst.TASK_NUMBER,
	                      String.valueOf(i));
				taskProps.put(InterfaceConst.TASKS_MAX,
	                      String.valueOf(maxTasks));
				taskProps.put(InterfaceConst.TOPIC, 
						  topics.get(i % topicsLength));
	    
				taskConfigs.add(taskProps);
			}
			return taskConfigs;		
		} catch (Exception e) {
			LOG.error(e.getMessage(), e);
			throw new ConnectException(ResourceBundleUtil.get(MessageKey.CONFIGURATION_EXCEPTION), e);
		}
	}
	 
	
	@Override
	public String version() {
		return AppInfoParser.getVersion();
	}
	
	/***
	 * Get the number of topics from config file
	 * @return
	 */
	private int numberOfTopics() {
		return configProperties.get(InterfaceConst.TOPIC).split(",").length;
	}
	
	/***
	 * Generate topics to be consumed considering number of 
	 * tasks 
	 * @param maxTasks - max number of tasks
	 * @return
	 */
	private ArrayList<String> balanceTopicsTasks(int maxTasks) {
		// Get topics
		String[] topics = configProperties.get(InterfaceConst.TOPIC).split(",");
		
		int topicsLength = numberOfTopics();  
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
