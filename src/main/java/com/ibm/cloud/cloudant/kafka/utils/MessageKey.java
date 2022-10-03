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
package com.ibm.cloud.cloudant.kafka.utils;

public class MessageKey {
    public static final String VALIDATION_AUTH_MUST_BE_SET = "ValidationAuthMustBeSet";
    public static final String VALIDATION_AUTH_BOTH_MUST_BE_SET = "ValidationAuthBothMustBeSet";
    public static final String VALIDATION_AUTH_AT_LEAST_ONE_MUST_BE_SET = "ValidationAuthAtLeastOneMustBeSet";
    public static final String VALIDATION_MUST_BE_ONE_OF = "ValidationMustBeOneOf";
    public static final String VALIDATION_NOT_A_URL = "ValidationNotAUrl";

    public static final String CLOUDANT_CONNECTION_URL_DOC = "CloudantConnectUrlDoc";
    public static final String CLOUDANT_CONNECTION_DB_DOC = "CloudantConnectDbDoc";
    public static final String CLOUDANT_CONNECTION_USR_DOC = "CloudantConnectUsrDoc";
    public static final String CLOUDANT_CONNECTION_PWD_DOC = "CloudantConnectPwdDoc";
    public static final String CLOUDANT_CONNECTION_AUTH_TYPE_DOC = "CloudantConnectAuthTypeDoc";
    public static final String CLOUDANT_CONNECTION_BEARER_TOKEN_DOC = "CloudantConnectBearerTokenDoc";
    public static final String CLOUDANT_CONNECTION_IAM_PROFILE_ID_DOC = "CloudantConnectIamProfileIdDoc";
    public static final String CLOUDANT_CONNECTION_IAM_PROFILE_NAME_DOC = "CloudantConnectIamProfileNameDoc";
    public static final String CLOUDANT_CONNECTION_CR_TOKEN_FILENAME_DOC = "CloudantConnectCrTokenFilenameDoc";
    public static final String CLOUDANT_CONNECTION_IAM_PROFILE_CRN_DOC = "CloudantConnectIamProfileCrnDoc";
    public static final String CLOUDANT_CONNECTION_APIKEY_DOC = "CloudantConnectApikeyDoc";
    public static final String CLOUDANT_CONNECTION_AUTH_URL_DOC = "CloudantConnectAuthUrlDoc";
    public static final String CLOUDANT_CONNECTION_SCOPE_DOC = "CloudantConnectScopeDoc";
    public static final String CLOUDANT_CONNECTION_CLIENT_ID_DOC = "CloudantConnectClientIdDoc";
    public static final String CLOUDANT_CONNECTION_CLIENT_SECRET_DOC = "CloudantConnectClientSecretDoc";
    public static final String CLOUDANT_BATCH_SIZE_SOURCE_DOC = "CloudantBatchSizeSourceDoc";
    public static final String CLOUDANT_BATCH_SIZE_SINK_DOC = "CloudantBatchSizeSinkDoc";

    public static final String CLOUDANT_CONNECTION_URL_DISP = "CloudantConnectUrlDisp";
    public static final String CLOUDANT_CONNECTION_DB_DISP = "CloudantConnectDbDisp";
    public static final String CLOUDANT_CONNECTION_USR_DISP = "CloudantConnectUsrDisp";
    public static final String CLOUDANT_CONNECTION_PWD_DISP = "CloudantConnectPwdDisp";
    public static final String CLOUDANT_CONNECTION_AUTH_TYPE_DISP = "CloudantConnectAuthTypeDisp";
    public static final String CLOUDANT_CONNECTION_BEARER_TOKEN_DISP = "CloudantConnectBearerTokenDisp";
    public static final String CLOUDANT_CONNECTION_IAM_PROFILE_ID_DISP = "CloudantConnectIamProfileIdDisp";
    public static final String CLOUDANT_CONNECTION_IAM_PROFILE_NAME_DISP = "CloudantConnectIamProfileNameDisp";
    public static final String CLOUDANT_CONNECTION_CR_TOKEN_FILENAME_DISP = "CloudantConnectCrTokenFilenameDisp";
    public static final String CLOUDANT_CONNECTION_IAM_PROFILE_CRN_DISP = "CloudantConnectIamProfileCrnDisp";
    public static final String CLOUDANT_CONNECTION_APIKEY_DISP = "CloudantConnectApikeyDisp";
    public static final String CLOUDANT_CONNECTION_AUTH_URL_DISP = "CloudantConnectAuthUrlDisp";
    public static final String CLOUDANT_CONNECTION_SCOPE_DISP = "CloudantConnectScopeDisp";
    public static final String CLOUDANT_CONNECTION_CLIENT_ID_DISP = "CloudantConnectClientIdDisp";
    public static final String CLOUDANT_CONNECTION_CLIENT_SECRET_DISP = "CloudantConnectClientSecretDisp";
    public static final String CLOUDANT_LAST_SEQ_NUM_DOC = "CloudantLastSeqNumDoc";
    public static final String CLOUDANT_LAST_SEQ_NUM_DISP = "CloudantLastSeqNumDisp";
    public static final String CLOUDANT_BATCH_SIZE_DISP = "CloudantBatchSizeDisp";

    public static final String KAFKA_TOPIC_LIST_DOC = "KafkaTopicListDoc";
    public static final String KAFKA_TOPIC_LIST_DISP = "KafkaTopicListDisp";

    public static final String CLOUDANT_TRANSFORM_FILTER_RECORD = "CloudantTransformFilterRecord";

    public static final String CLOUDANT_ARRAY_DELIMITER_DOC = "CloudantArrayDelimiterDoc";
    public static final String CLOUDANT_ARRAY_PURPOSE = "CloudantArrayPurpose";

    public static final String CLOUDANT_STRUCT_PURPOSE = "CloudantStructPurpose";
    public static final String CLOUDANT_STRUCT_UNDETECTABLE_ARRAY = "CloudantStructUndetectableArrayType";
    public static final String CLOUDANT_STRUCT_MIXED_TYPE_ARRAY = "CloudantStructMixedTypeArray";
    public static final String CLOUDANT_STRUCT_UNHANDLED_TYPE = "CloudantStructUnhandledType";
    public static final String CLOUDANT_STRUCT_UNKNOWN_TYPE = "CloudantStructUnknownType";

    public static final String CLOUDANT_KEY_NO_ID =	"CloudantKeyNoId";

}
