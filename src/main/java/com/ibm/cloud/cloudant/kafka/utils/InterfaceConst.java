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

public class InterfaceConst {

    public final static String URL = "cloudant.url";
    public final static String DB = "cloudant.db";
    public final static String USERNAME = "cloudant.username";
    public final static String PASSWORD = "cloudant.password";
    public final static String BEARER_TOKEN = "cloudant.bearer.token";
    public final static String IAM_PROFILE_ID = "cloudant.iam.profile.id";
    public final static String IAM_PROFILE_NAME = "cloudant.iam.profile.name";
    public final static String CR_TOKEN_FILENAME = "cloudant.cr.token.filename";
    public final static String IAM_PROFILE_CRN = "cloudant.iam.profile.crn";
    public final static String AUTH_URL = "cloudant.auth.url";
    public final static String SCOPE = "cloudant.scope";
    public final static String CLIENT_ID = "cloudant.client.id";
    public final static String CLIENT_SECRET = "cloudant.client.secret";
    public final static String APIKEY = "cloudant.apikey";
    public final static String AUTH_TYPE = "cloudant.auth.type";

    public final static String LAST_CHANGE_SEQ = "cloudant.since";

    public final static String DESIGN_DOC = "cloudant.design.doc";
    public final static String VIEW_NAME = "cloudant.view.name";

    public final static String TOPIC = "topics";

    public final static String TASKS_MAX = "tasks.max";
    public final static String BATCH_SIZE = "batch.size";

    public static final int DEFAULT_BATCH_SIZE_SOURCE = 1000;
    public static final int BATCH_SIZE_MIN_SOURCE = 1;
    public static final int BATCH_SIZE_MAX_SOURCE = 10000;
    public static final int DEFAULT_BATCH_SIZE_SINK = 1000;
    public static final int BATCH_SIZE_MIN_SINK = 1;
    public static final int BATCH_SIZE_MAX_SINK = 2000;
}
