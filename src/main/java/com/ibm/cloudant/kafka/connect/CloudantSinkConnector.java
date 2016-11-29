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
import org.apache.kafka.connect.sink.SinkConnector;
import org.apache.log4j.Logger;

import com.ibm.cloudant.kafka.common.MessageKey;
import com.ibm.cloudant.kafka.common.utils.ResourceBundleUtil;

public class CloudantSinkConnector extends SinkConnector {

	private static Logger LOG = Logger.getLogger(CloudantSinkConnector.class);
	
	private Map<String, String> configProperties;
	private CloudantSinkConnectorConfig config;

	@Override
	public ConfigDef config() {
		return CloudantSourceConnectorConfig.CONFIG_DEF;
	}

	
	@Override
	public String version() {
		return AppInfoParser.getVersion();
	}

	@Override
	public void start(Map<String, String> props) {
	     configProperties = props;
	     config = new CloudantSinkConnectorConfig(configProperties);	
	}

	@Override
	public Class<? extends Task> taskClass() {
			return CloudantSinkTask.class;
	}

	@Override
	public List<Map<String, String>> taskConfigs(int maxTasks) {
		try {
			Map<String, String> taskProps = new HashMap<String, String>(configProperties);
			List<Map<String, String>> taskConfigs = new ArrayList<Map<String, String>>(maxTasks);

			for (int i = 0; i < maxTasks; ++i) {
				// add task specific properties here (if any)
				// taskProps.put(CloudantSourceTaskConfig.PROPERTY, value);

				taskConfigs.add(taskProps);
			}
			return taskConfigs;		
		} catch (Exception e) {
			LOG.error(e.getMessage(), e);
			throw new ConnectException(ResourceBundleUtil.get(MessageKey.CONFIGURATION_EXCEPTION), e);
		}
	}

	@Override
	public void stop() {
		// TODO Auto-generated method stub

	}

}
