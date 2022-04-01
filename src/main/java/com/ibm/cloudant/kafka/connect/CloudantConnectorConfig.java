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

package com.ibm.cloudant.kafka.connect;

import java.util.Map;

import com.ibm.cloud.cloudant.security.CouchDbSessionAuthenticator;
import com.ibm.cloud.sdk.core.security.Authenticator;
import com.ibm.cloudant.kafka.common.InterfaceConst;
import com.ibm.cloudant.kafka.common.MessageKey;
import com.ibm.cloudant.kafka.common.utils.ResourceBundleUtil;

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
        Authenticator.AUTHTYPE_NOAUTH
        );
    protected static final String USERNAME_DEFAULT = null;
    protected static final String PASSWORD_DEFAULT = null;
    protected static final String APIKEY_DEFAULT = null;
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
                        USERNAME_DEFAULT,
                        Importance.HIGH,
                        ResourceBundleUtil.get(MessageKey.CLOUDANT_CONNECTION_USR_DOC),
                        DATABASE_GROUP,
                        1,
                        Width.LONG,
                        ResourceBundleUtil.get(MessageKey.CLOUDANT_CONNECTION_USR_DISP))
                // Cloudant Password
                .define(InterfaceConst.PASSWORD,
                        Type.PASSWORD,
                        PASSWORD_DEFAULT,
                        Importance.HIGH,
                        ResourceBundleUtil.get(MessageKey.CLOUDANT_CONNECTION_PWD_DOC),
                        DATABASE_GROUP,
                        1,
                        Width.LONG,
                        ResourceBundleUtil.get(MessageKey.CLOUDANT_CONNECTION_PWD_DISP))
                // Cloudant API key
                .define(InterfaceConst.APIKEY,
                        Type.PASSWORD,
                        APIKEY_DEFAULT,
                        Importance.HIGH,
                        ResourceBundleUtil.get(MessageKey.CLOUDANT_CONNECTION_APIKEY_DOC),
                        DATABASE_GROUP,
                        1,
                        Width.LONG,
                        ResourceBundleUtil.get(MessageKey.CLOUDANT_CONNECTION_APIKEY_DISP))
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
