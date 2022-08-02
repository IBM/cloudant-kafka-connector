/*
 * Copyright © 2022 IBM Corp. All rights reserved.
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

package com.ibm.cloud.cloudant.kafka.connect;

import java.util.Map;

import com.ibm.cloud.cloudant.security.CouchDbSessionAuthenticator;
import com.ibm.cloud.sdk.core.security.Authenticator;
import com.ibm.cloud.cloudant.kafka.common.InterfaceConst;
import com.ibm.cloud.cloudant.kafka.common.MessageKey;
import com.ibm.cloud.cloudant.kafka.common.utils.ResourceBundleUtil;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;

public class CloudantConnectorConfig extends AbstractConfig {

    protected static final String DATABASE_GROUP = "Database";
    protected static final String AUTH_TYPE_DEFAULT = Authenticator.AUTHTYPE_IAM;
    protected static final ListRecommender VALID_AUTHS = new ListRecommender(
        Authenticator.AUTHTYPE_IAM,
        CouchDbSessionAuthenticator.AUTH_TYPE,
        Authenticator.AUTHTYPE_BASIC,
        Authenticator.AUTHTYPE_NOAUTH,
        Authenticator.AUTHTYPE_BEARER_TOKEN,
        Authenticator.AUTHTYPE_CONTAINER,
        Authenticator.AUTHTYPE_VPC
        );
    protected static final String NULL_DEFAULT = null; // null default indicates it could be optional (depending on other config options)
    protected static final String LAST_SEQ_NUM_DEFAULT = "0";

    public static final ConfigDef CONFIG_DEF = baseConfigDef();

    public static ConfigDef baseConfigDef() {

        return new ConfigDef()

                // Cloudant URL
                .define(InterfaceConst.URL,
                        Type.STRING,
                        ConfigDef.NO_DEFAULT_VALUE,
                        new UrlValidator(),
                        Importance.HIGH,
                        ResourceBundleUtil.get(MessageKey.CLOUDANT_CONNECTION_URL_DOC),
                        DATABASE_GROUP,
                        1,
                        Width.LONG,
                        ResourceBundleUtil.get(MessageKey.CLOUDANT_CONNECTION_URL_DISP))
                // Cloudant DB
                .define(InterfaceConst.DB,
                        Type.STRING,
                        Importance.HIGH,
                        ResourceBundleUtil.get(MessageKey.CLOUDANT_CONNECTION_DB_DOC),
                        DATABASE_GROUP,
                        1,
                        Width.LONG,
                        ResourceBundleUtil.get(MessageKey.CLOUDANT_CONNECTION_DB_DISP))
                // Cloudant Username
                .define(InterfaceConst.USERNAME,
                        Type.STRING,
                        NULL_DEFAULT,
                        Importance.HIGH,
                        ResourceBundleUtil.get(MessageKey.CLOUDANT_CONNECTION_USR_DOC),
                        DATABASE_GROUP,
                        1,
                        Width.LONG,
                        ResourceBundleUtil.get(MessageKey.CLOUDANT_CONNECTION_USR_DISP))
                // Cloudant Password
                .define(InterfaceConst.PASSWORD,
                        Type.PASSWORD,
                        NULL_DEFAULT,
                        Importance.HIGH,
                        ResourceBundleUtil.get(MessageKey.CLOUDANT_CONNECTION_PWD_DOC),
                        DATABASE_GROUP,
                        1,
                        Width.LONG,
                        ResourceBundleUtil.get(MessageKey.CLOUDANT_CONNECTION_PWD_DISP))
                // Cloudant API key
                .define(InterfaceConst.APIKEY,
                        Type.PASSWORD,
                        NULL_DEFAULT,
                        Importance.HIGH,
                        ResourceBundleUtil.get(MessageKey.CLOUDANT_CONNECTION_APIKEY_DOC),
                        DATABASE_GROUP,
                        1,
                        Width.LONG,
                        ResourceBundleUtil.get(MessageKey.CLOUDANT_CONNECTION_APIKEY_DISP))
                // Cloudant bearer token
                .define(InterfaceConst.BEARER_TOKEN,
                        Type.STRING,
                        NULL_DEFAULT,
                        Importance.HIGH,
                        ResourceBundleUtil.get(MessageKey.CLOUDANT_CONNECTION_BEARER_TOKEN_DOC),
                        DATABASE_GROUP,
                        1,
                        Width.LONG,
                        ResourceBundleUtil.get(MessageKey.CLOUDANT_CONNECTION_BEARER_TOKEN_DISP))
                // Cloudant IAM profile id
                .define(InterfaceConst.IAM_PROFILE_ID,
                        Type.STRING,
                        NULL_DEFAULT,
                        Importance.HIGH,
                        ResourceBundleUtil.get(MessageKey.CLOUDANT_CONNECTION_IAM_PROFILE_ID_DOC),
                        DATABASE_GROUP,
                        1,
                        Width.LONG,
                        ResourceBundleUtil.get(MessageKey.CLOUDANT_CONNECTION_IAM_PROFILE_ID_DISP))
                // Cloudant IAM profile name
                .define(InterfaceConst.IAM_PROFILE_NAME,
                        Type.STRING,
                        NULL_DEFAULT,
                        Importance.HIGH,
                        ResourceBundleUtil.get(MessageKey.CLOUDANT_CONNECTION_IAM_PROFILE_NAME_DOC),
                        DATABASE_GROUP,
                        1,
                        Width.LONG,
                        ResourceBundleUtil.get(MessageKey.CLOUDANT_CONNECTION_IAM_PROFILE_NAME_DISP))
                // Cloudant CR token filename
                .define(InterfaceConst.CR_TOKEN_FILENAME,
                        Type.STRING,
                        NULL_DEFAULT,
                        Importance.HIGH,
                        ResourceBundleUtil.get(MessageKey.CLOUDANT_CONNECTION_CR_TOKEN_FILENAME_DOC),
                        DATABASE_GROUP,
                        1,
                        Width.LONG,
                        ResourceBundleUtil.get(MessageKey.CLOUDANT_CONNECTION_CR_TOKEN_FILENAME_DISP))
                // Cloudant IAM profile CRN
                .define(InterfaceConst.IAM_PROFILE_CRN,
                        Type.STRING,
                        NULL_DEFAULT,
                        Importance.HIGH,
                        ResourceBundleUtil.get(MessageKey.CLOUDANT_CONNECTION_IAM_PROFILE_CRN_DOC),
                        DATABASE_GROUP,
                        1,
                        Width.LONG,
                        ResourceBundleUtil.get(MessageKey.CLOUDANT_CONNECTION_IAM_PROFILE_CRN_DISP))
                // Cloudant auth url
                .define(InterfaceConst.AUTH_URL,
                        Type.STRING,
                        NULL_DEFAULT,
                        Importance.HIGH,
                        ResourceBundleUtil.get(MessageKey.CLOUDANT_CONNECTION_AUTH_URL_DOC),
                        DATABASE_GROUP,
                        1,
                        Width.LONG,
                        ResourceBundleUtil.get(MessageKey.CLOUDANT_CONNECTION_AUTH_URL_DISP))
                // Cloudant scope
                .define(InterfaceConst.SCOPE,
                        Type.STRING,
                        NULL_DEFAULT,
                        Importance.HIGH,
                        ResourceBundleUtil.get(MessageKey.CLOUDANT_CONNECTION_SCOPE_DOC),
                        DATABASE_GROUP,
                        1,
                        Width.LONG,
                        ResourceBundleUtil.get(MessageKey.CLOUDANT_CONNECTION_SCOPE_DISP))
                // Cloudant client id
                .define(InterfaceConst.CLIENT_ID,
                        Type.STRING,
                        NULL_DEFAULT,
                        Importance.HIGH,
                        ResourceBundleUtil.get(MessageKey.CLOUDANT_CONNECTION_CLIENT_ID_DOC),
                        DATABASE_GROUP,
                        1,
                        Width.LONG,
                        ResourceBundleUtil.get(MessageKey.CLOUDANT_CONNECTION_CLIENT_ID_DISP))
                // Cloudant client secret
                .define(InterfaceConst.CLIENT_SECRET,
                        Type.STRING,
                        NULL_DEFAULT,
                        Importance.HIGH,
                        ResourceBundleUtil.get(MessageKey.CLOUDANT_CONNECTION_CLIENT_SECRET_DOC),
                        DATABASE_GROUP,
                        1,
                        Width.LONG,
                        ResourceBundleUtil.get(MessageKey.CLOUDANT_CONNECTION_CLIENT_SECRET_DISP))
                // Cloudant auth type
                .define(InterfaceConst.AUTH_TYPE,
                        Type.STRING,
                        AUTH_TYPE_DEFAULT,
                        VALID_AUTHS,
                        Importance.HIGH,
                        ResourceBundleUtil.get(MessageKey.CLOUDANT_CONNECTION_AUTH_TYPE_DOC),
                        DATABASE_GROUP,
                        1,
                        Width.LONG,
                        ResourceBundleUtil.get(MessageKey.CLOUDANT_CONNECTION_AUTH_TYPE_DISP),
                        VALID_AUTHS)
                // Kafka topic
                .define(InterfaceConst.TOPIC,
                        Type.LIST,
                        Importance.HIGH,
                        ResourceBundleUtil.get(MessageKey.KAFKA_TOPIC_LIST_DOC),
                        DATABASE_GROUP,
                        1,
                        Width.LONG,
                        ResourceBundleUtil.get(MessageKey.KAFKA_TOPIC_LIST_DISP));
    }

    public CloudantConnectorConfig(ConfigDef definition, Map<?, ?> originals, boolean doLog) {
        super(definition, originals, doLog);
    }

    public CloudantConnectorConfig(ConfigDef definition, Map<?, ?> originals) {
        super(definition, originals);
    }

}
