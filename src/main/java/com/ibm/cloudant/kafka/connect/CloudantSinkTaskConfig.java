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

import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;

import com.ibm.cloudant.kafka.common.CloudantConst;
import com.ibm.cloudant.kafka.common.InterfaceConst;

public class CloudantSinkTaskConfig extends CloudantSinkConnectorConfig {

	// Expand this ConfigDef with task specific parameters	
	static org.apache.kafka.common.config.ConfigDef config = baseConfigDef()
		      .define(InterfaceConst.BATCH_SIZE, Type.INT, CloudantConst.DEFAULT_BATCH_SIZE,
		    		  Importance.LOW, InterfaceConst.BATCH_SIZE)
		      .define(InterfaceConst.GUID_SCHEMA, Type.STRING, InterfaceConst.DEFAULT_GUID_SETTING,
		    		  Importance.LOW, InterfaceConst.GUID_SCHEMA);
	
	public CloudantSinkTaskConfig(Map<String, String> originals) {
		super(config, originals);
	}

}
