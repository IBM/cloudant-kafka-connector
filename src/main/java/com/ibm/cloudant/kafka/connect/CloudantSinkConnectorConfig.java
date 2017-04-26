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

import java.util.Map;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;
import org.apache.log4j.Logger;

import com.ibm.cloudant.kafka.common.InterfaceConst;
import com.ibm.cloudant.kafka.common.MessageKey;
import com.ibm.cloudant.kafka.common.utils.ResourceBundleUtil;

public class CloudantSinkConnectorConfig extends AbstractConfig {
	
	private static Logger LOG = Logger.getLogger(CloudantSinkConnectorConfig.class);

	public static final String DATABASE_GROUP = "Database";
	public static final String CLOUDANT_LAST_SEQ_NUM_DEFAULT = "0";
	
	public static final ConfigDef CONFIG_DEF = baseConfigDef();

	public static ConfigDef baseConfigDef() {
		
		return new ConfigDef()
				
				  // Kafka topic
				  .define(InterfaceConst.TOPIC, Type.LIST,
						  Importance.HIGH, 
						  ResourceBundleUtil.get(MessageKey.KAFKA_TOPIC_LIST_DOC), 
						  DATABASE_GROUP, 1, Width.LONG,
						  ResourceBundleUtil.get(MessageKey.KAFKA_TOPIC_LIST_DISP))
		
				  // Cloudant URL
				  .define(InterfaceConst.URL, Type.STRING, Importance.HIGH, 
						  ResourceBundleUtil.get(MessageKey.CLOUDANT_CONNECTION_URL_DOC), 
						  DATABASE_GROUP, 1, Width.LONG,
						  ResourceBundleUtil.get(MessageKey.CLOUDANT_CONNECTION_URL_DISP))
				  // Cloudant Username
				  .define(InterfaceConst.USER_NAME, Type.STRING, Importance.HIGH, 
						  ResourceBundleUtil.get(MessageKey.CLOUDANT_CONNECTION_USR_DOC), 
						  DATABASE_GROUP, 1, Width.LONG,
						  ResourceBundleUtil.get(MessageKey.CLOUDANT_CONNECTION_USR_DISP))
				  // Cloudant Password
				  .define(InterfaceConst.PASSWORD, Type.STRING, Importance.HIGH, 
						  ResourceBundleUtil.get(MessageKey.CLOUDANT_CONNECTION_PWD_DOC), 
						  DATABASE_GROUP, 1, Width.LONG,
						  ResourceBundleUtil.get(MessageKey.CLOUDANT_CONNECTION_PWD_DISP))
				  // Cloudant last change sequence
				  .define(InterfaceConst.LAST_CHANGE_SEQ, Type.STRING, CLOUDANT_LAST_SEQ_NUM_DEFAULT, 
						  Importance.LOW, 
						  ResourceBundleUtil.get(MessageKey.CLOUDANT_LAST_SEQ_NUM_DOC), 
						  DATABASE_GROUP, 1, Width.LONG,
						  ResourceBundleUtil.get(MessageKey.CLOUDANT_LAST_SEQ_NUM_DOC));
				
	}
	
	public CloudantSinkConnectorConfig(Map<String, String> originals) {
		super(CONFIG_DEF, originals, false);
	}
	
	protected CloudantSinkConnectorConfig(ConfigDef subclassConfigDef, Map<String, String> originals) {
	    super(subclassConfigDef, originals);
	}
	
	public static void main(String[] args) {
		  System.out.println(CONFIG_DEF.toRst());
	}
}
