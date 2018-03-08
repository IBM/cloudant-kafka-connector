/*
 * Copyright © 2016, 2018 IBM Corp. All rights reserved.
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

import com.ibm.cloudant.kafka.common.InterfaceConst;
import com.ibm.cloudant.kafka.common.MessageKey;
import com.ibm.cloudant.kafka.common.utils.ResourceBundleUtil;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;
import org.apache.log4j.Logger;

import java.util.Map;

public class CloudantSourceConnectorConfig extends AbstractConfig {

    private static Logger LOG = Logger.getLogger(CloudantSourceConnectorConfig.class);

    public static final String DATABASE_GROUP = "Database";
    public static final String CLOUDANT_LAST_SEQ_NUM_DEFAULT = null;
    public static final ConfigDef CONFIG_DEF = baseConfigDef();

    public static ConfigDef baseConfigDef() {
        return new ConfigDef()

                // Cloudant URL
                .define(InterfaceConst.URL, Type.STRING, Importance.HIGH,
                        ResourceBundleUtil.get(MessageKey.CLOUDANT_CONNECTION_URL_DOC),
                        DATABASE_GROUP, 1, Width.LONG,
                        ResourceBundleUtil.get(MessageKey.CLOUDANT_CONNECTION_URL_DISP))
                // Cloudant Username
                .define(InterfaceConst.USER_NAME, Type.STRING, Importance.HIGH,
                        ResourceBundleUtil.get(MessageKey.CLOUDANT_CONNECTION_USR_DOC),
                        DATABASE_GROUP, 1, Width.LONG,
                        ResourceBundleUtil.get(MessageKey.CLOUDANT_CONNECTION_USR_DISP))
                // Cloudant Password
                .define(InterfaceConst.PASSWORD, Type.PASSWORD, Importance.HIGH,
                        ResourceBundleUtil.get(MessageKey.CLOUDANT_CONNECTION_PWD_DOC),
                        DATABASE_GROUP, 1, Width.LONG,
                        ResourceBundleUtil.get(MessageKey.CLOUDANT_CONNECTION_PWD_DISP))
                // Cloudant last change sequence
                .define(InterfaceConst.LAST_CHANGE_SEQ, Type.STRING, CLOUDANT_LAST_SEQ_NUM_DEFAULT,
                        Importance.LOW,
                        ResourceBundleUtil.get(MessageKey.CLOUDANT_LAST_SEQ_NUM_DOC),
                        DATABASE_GROUP, 1, Width.LONG,
                        ResourceBundleUtil.get(MessageKey.CLOUDANT_LAST_SEQ_NUM_DOC))

                // Kafka topic
                .define(InterfaceConst.TOPIC, Type.LIST,
                        Importance.HIGH,
                        ResourceBundleUtil.get(MessageKey.KAFKA_TOPIC_LIST_DOC),
                        DATABASE_GROUP, 1, Width.LONG,
                        ResourceBundleUtil.get(MessageKey.KAFKA_TOPIC_LIST_DISP))

                // Whether to omit design documents
                .define(InterfaceConst.OMIT_DESIGN_DOCS, Type.BOOLEAN,
                        false,
                        Importance.LOW,
                        ResourceBundleUtil.get(MessageKey.CLOUDANT_OMIT_DDOC_DOC),
                        DATABASE_GROUP, 1, Width.NONE,
                        ResourceBundleUtil.get(MessageKey.CLOUDANT_OMIT_DDOC_DISP))

                // Whether to generate a struct Schema
                .define(InterfaceConst.USE_VALUE_SCHEMA_STRUCT, Type.BOOLEAN,
                        false,
                        Importance.MEDIUM,
                        ResourceBundleUtil.get(MessageKey.CLOUDANT_STRUCT_SCHEMA_DOC),
                        DATABASE_GROUP, 1, Width.NONE,
                        ResourceBundleUtil.get(MessageKey.CLOUDANT_STRUCT_SCHEMA_DISP))

                // Whether to flatten nested objects in the generated struct
                .define(InterfaceConst.FLATTEN_VALUE_SCHEMA_STRUCT, Type.BOOLEAN,
                        false,
                        Importance.MEDIUM,
                        ResourceBundleUtil.get(MessageKey.CLOUDANT_STRUCT_SCHEMA_FLATTEN_DOC),
                        DATABASE_GROUP, 1, Width.NONE,
                        ResourceBundleUtil.get(MessageKey.CLOUDANT_STRUCT_SCHEMA_FLATTEN_DISP));
    }

    public CloudantSourceConnectorConfig(Map<String, String> originals) {
        super(CONFIG_DEF, originals, false);
    }

    protected CloudantSourceConnectorConfig(ConfigDef subclassConfigDef, Map<String, String> originals) {
        super(subclassConfigDef, originals);
    }

    public static void main(String[] args) {
        System.out.println(CONFIG_DEF.toRst());
    }

}
