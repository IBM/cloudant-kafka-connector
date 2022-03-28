/*
 * Copyright © 2016, 2022 IBM Corp. All rights reserved.
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
package com.ibm.cloudant.kafka.common;

public class InterfaceConst {
	
	public final static String URL = "cloudant.url";
	public final static String DB = "cloudant.db";
	public final static String USER_NAME = "cloudant.username";
	public final static String PASSWORD = "cloudant.password";
	public final static String AUTH_TYPE = "cloudant.auth.type";
	public final static String API_KEY = "cloudant.apikey";
	
	public final static String LAST_CHANGE_SEQ = "cloudant.since";

	public final static String TOPIC = "topics";
	
	public final static String TASKS_MAX = "tasks.max";
	public final static String TASK_NUMBER = "task.number";
	public final static String BATCH_SIZE = "batch.size";
		
	public static final int DEFAULT_BATCH_SIZE = 1000;
	public static final int DEFAULT_TASKS_MAX = 1;
	public static final int DEFAULT_TASK_NUMBER = 0;
	
	public final static String REPLICATION = "replication";	
	public final static Boolean DEFAULT_REPLICATION = false;
	public final static String KC_SCHEMA = "kc_schema";

	// Whether to omit design documents
	public final static String OMIT_DESIGN_DOCS = "cloudant.omit.design.docs";

	// Whether to use a struct schema for the message values
	public final static String USE_VALUE_SCHEMA_STRUCT = "cloudant.value.schema.struct";
	public final static String FLATTEN_VALUE_SCHEMA_STRUCT = "cloudant.value.schema.struct.flatten";
}
