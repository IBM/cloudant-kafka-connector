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

import com.ibm.cloudant.kafka.common.InterfaceConst;
import com.ibm.cloudant.kafka.common.MessageKey;
import com.ibm.cloudant.kafka.common.utils.ResourceBundleUtil;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;

public class CloudantConnectorConfig extends AbstractConfig {

    public static final ConfigDef CONFIG_DEF = baseConfigDef();

    public static final String DATABASE_GROUP = "Database";
    public static final String CLOUDANT_AUTH_TYPE_BASIC = "basic";
    public static final String CLOUDANT_AUTH_TYPE_DEFAULT = CLOUDANT_AUTH_TYPE_BASIC;
    public static final String CLOUDANT_USERNAME_DEFAULT = null;
    public static final String CLOUDANT_PASSWORD_DEFAULT = null;
    public static final String CLOUDANT_LAST_SEQ_NUM_DEFAULT = "0";

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
                .define(InterfaceConst.USER_NAME,
                        Type.STRING,
                        CLOUDANT_USERNAME_DEFAULT,
                        Importance.HIGH,
                        ResourceBundleUtil.get(MessageKey.CLOUDANT_CONNECTION_USR_DOC),
                        DATABASE_GROUP,
                        1,
                        Width.LONG,
                        ResourceBundleUtil.get(MessageKey.CLOUDANT_CONNECTION_USR_DISP))
                // Cloudant Password
                .define(InterfaceConst.PASSWORD,
                        Type.PASSWORD,
                        CLOUDANT_PASSWORD_DEFAULT,
                        Importance.HIGH,
                        ResourceBundleUtil.get(MessageKey.CLOUDANT_CONNECTION_PWD_DOC),
                        DATABASE_GROUP,
                        1,
                        Width.LONG,
                        ResourceBundleUtil.get(MessageKey.CLOUDANT_CONNECTION_PWD_DISP))
                // Cloudant auth type
                .define(InterfaceConst.AUTH_TYPE,
                        Type.STRING,
                        CLOUDANT_AUTH_TYPE_DEFAULT,
                        new ListRecommender(CLOUDANT_AUTH_TYPE_BASIC),
                        Importance.LOW,
                        ResourceBundleUtil.get(MessageKey.CLOUDANT_CONNECTION_AUTH_TYPE_DOC),
                        DATABASE_GROUP,
                        1,
                        Width.LONG,
                        ResourceBundleUtil.get(MessageKey.CLOUDANT_CONNECTION_AUTH_TYPE_DISP),
                        new ListRecommender(CLOUDANT_AUTH_TYPE_BASIC))
                // Cloudant last change sequence
                .define(InterfaceConst.LAST_CHANGE_SEQ,
                        Type.STRING,
                        CLOUDANT_LAST_SEQ_NUM_DEFAULT,
                        Importance.LOW,
                        ResourceBundleUtil.get(MessageKey.CLOUDANT_LAST_SEQ_NUM_DOC),
                        DATABASE_GROUP,
                        1,
                        Width.LONG,
                        ResourceBundleUtil.get(MessageKey.CLOUDANT_LAST_SEQ_NUM_DOC))
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
