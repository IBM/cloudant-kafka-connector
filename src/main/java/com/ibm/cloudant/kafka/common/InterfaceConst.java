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
package com.ibm.cloudant.kafka.common;

public class InterfaceConst {
	
	public final static String URL = "cloudant.db.url";
	public final static String USER_NAME = "cloudant.db.username";
	public final static String PASSWORD = "cloudant.db.password";
	
	public final static String LAST_CHANGE_SEQ = "cloudant.db.since";

	public final static String TOPIC = "topics";
	
	public final static String TASKS_MAX = "tasks.max";
	public final static String TASK_NUMBER = "task.number";
	public final static String BATCH_SIZE = "batch.size";

	public final static String GUID_SCHEMA = "guid.schema";	
	
	public static final String DEFAULT_GUID_SETTING = InterfaceConst.GUID_SETTING.KAFKA.name();	
	public enum GUID_SETTING {
		KAFKA, CLOUDANT
	};

}
