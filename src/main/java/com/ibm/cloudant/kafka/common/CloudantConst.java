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

public class CloudantConst {
	public static final String CLOUDANT_RESULTS = "results";
	public static final String CLOUDANT_TOTAL_ROWS = "total_rows";
	public static final String CLOUDANT_ROWS = "rows";
	public static final String CLOUDANT_VALUE = "value";
	public static final String CLOUDANT_DOC = "doc";

	public static final String CLOUDANT_DOC_ID = "_id";
	public static final String CLOUDANT_REV = "_rev";
	public static final String DELETED = "deleted";

	public static final String RESPONSE_ID = "id";
	public static final String RESPONSE_REV = "rev";
	public static final String RESPONSE_ERROR = "error";
	public static final String RESPONSE_REASON = "reason";
	public static final String RESPONSE_OK = "ok";
	public static final String RESPONSE_TOTAL_ROWS = "total_rows";
	public static final String RESPONSE_OFFSET = "offset";
	public static final String RESPONSE_ROWS = "rows";
	public static final String RESPONSE_CODE = "response_code";

	public static final String DESCRIPTION = "description";

	public static final String QUERY = "query";
	public static final String DESIGN_DOC_ID = "_id";
	public static final String DESIGN_DOC_MAP = "map";
	public static final String DESIGN_DOC_VIEWS = "views";
	public static final String DESIGN_DOC_TASKS = "tasks";
	public static final String DESIGN_DOC_LANGUAGE = "language";
	public static final String DESIGN_DOC_ANALYZER = "analyzer";
	public static final String DESIGN_DOC_INDEX = "index";
	public static final String DESIGN_DOC_INDEXS = "indexes";
	public final static String SUCCESS_STATUS = "ok";
	public static final String UPDATE_SEQ = "update_seq";
	public static final String DOC_COUNT = "doc_count";
	public final static String CLOUDANT_DOCS = "docs";
	public final static String RESULT_REV = "rev";
	public final static String INCLUDE_DOCS = "include_docs";
	public final static String SEQ = "seq";
	public final static String LAST_SEQ = "last_seq";
	public final static String CLOUDANT_DELETED = "_deleted";
	public final static String DESIGN_PRIFIX = "_design/";
	public final static String ATTRIBUTE_ATTACHEMENT = "_attachments";

	public final static int    CLOUDANT_RECONNECT_MAX_COUNT = 3;
	public final static int    CLOUDANT_RECONNECT_WAIT_TIME = 1;
	public final static int    CLOUDANT_RECONNECTS_PER_CYCLE = 10;

	public final static String UNIT_OF_WORK_NUMBER = "unitOfWokNumber";
	public static final String DATA_SIZE = "data_size";
	public static final String OTHER = "other";
	public static final Object CLOUDANT_DOC_COUNT = "doc_count";
	public static final String SHARDS = "shards";
	
	//10 seconds
	public static final int RECONNECT_INTERVAL = 10;
	//86400 seconds(24 hours)
	public static final int RECONNECT_MAX_WAIT_TIME = 86400;
	
	public static final int DEFAULT_BATCH_SIZE = 1000;

}