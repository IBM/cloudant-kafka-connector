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

public class MessageKey {
	public static final String CONFIGURATION_EXCEPTION =  "ConfigurationException";
	public static final String READ_CLOUDANT_STREAM_ERROR = "ReadCloudantStreamError";
	public static final String CLOUDANT_DATABASE_ERROR = "CloudantDatabaseError";
	public static final String STREAM_CLOSED_ERROR = "ClosedStreamError";
	public static final String STREAM_TERMINATE_ERROR = "TerminateStreamError";
	
	public static final String CLOUDANT_CONNECTION_URL_DOC = "CloudantConnectUrlDoc";
	public static final String CLOUDANT_CONNECTION_USR_DOC = "CloudantConnectUsrDoc";
	public static final String CLOUDANT_CONNECTION_PWD_DOC  ="CloudantConnectPwdDoc";
	public static final String CLOUDANT_LAST_SEQ_NUM_DOC = "CloudantLastSeqNumDoc";
	
	public static final String CLOUDANT_CONNECTION_URL_DISP = "CloudantConnectUrlDisp";
	public static final String CLOUDANT_CONNECTION_USR_DISP = "CloudantConnectUsrDisp";
	public static final String CLOUDANT_CONNECTION_PWD_DISP = "CloudantConnectPwdDisp";
	public static final String CLOUDANT_LAST_SEQ_NUM_DISP = "CloudantLastSeqNumDisp";
	
	public static final String KAFKA_TOPIC_LIST_DOC = "KafkaTopicListDoc";
	public static final String KAFKA_TOPIC_LIST_DISP = "KafkaTopicListDisp";
	
	public static final String GUID_SCHEMA = "GuidSchema";
}
