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
package com.ibm.cloud.cloudant.kafka.connect;

import com.ibm.cloud.cloudant.kafka.common.InterfaceConst;
import com.ibm.cloud.cloudant.kafka.common.MessageKey;
import com.ibm.cloud.cloudant.kafka.common.utils.ResourceBundleUtil;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;

import java.util.Map;

public class CloudantSourceConnectorConfig extends CloudantConnectorConfig {

    public static final ConfigDef CONFIG_DEF = baseConfigDef();

    public static ConfigDef baseConfigDef() {
        return new ConfigDef(CloudantConnectorConfig.CONFIG_DEF)
                // Cloudant last change sequence
                .define(InterfaceConst.LAST_CHANGE_SEQ,
                        Type.STRING,
                        LAST_SEQ_NUM_DEFAULT,
                        Importance.LOW,
                        ResourceBundleUtil.get(MessageKey.CLOUDANT_LAST_SEQ_NUM_DOC),
                        DATABASE_GROUP,
                        1,
                        Width.LONG,
                        ResourceBundleUtil.get(MessageKey.CLOUDANT_LAST_SEQ_NUM_DOC))
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

}
