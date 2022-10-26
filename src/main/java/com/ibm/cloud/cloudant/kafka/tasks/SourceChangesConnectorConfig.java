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
package com.ibm.cloud.cloudant.kafka.tasks;

import com.ibm.cloud.cloudant.kafka.utils.InterfaceConst;
import com.ibm.cloud.cloudant.kafka.utils.MessageKey;
import com.ibm.cloud.cloudant.kafka.utils.ResourceBundleUtil;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;

import java.util.Map;

public class SourceChangesConnectorConfig extends ConnectorConfig {

    public static final ConfigDef CONFIG_DEF = baseConfigDef();

    public static ConfigDef baseConfigDef() {

        int order = 100; // pick a high number so these will come after those from base config def

        return new ConfigDef(ConnectorConfig.CONFIG_DEF)
                // batch size
                .define(InterfaceConst.BATCH_SIZE,
                        Type.INT,
                        InterfaceConst.DEFAULT_BATCH_SIZE_SOURCE,
                        ConfigDef.Range.between(InterfaceConst.BATCH_SIZE_MIN_SOURCE, InterfaceConst.BATCH_SIZE_MAX_SOURCE),
                        Importance.MEDIUM,
                        ResourceBundleUtil.get(MessageKey.CLOUDANT_BATCH_SIZE_SOURCE_DOC),
                        KAFKA_GROUP,
                        order++,
                        Width.SHORT,
                        ResourceBundleUtil.get(MessageKey.CLOUDANT_BATCH_SIZE_DISP))
                // Cloudant last change sequence
                .define(InterfaceConst.LAST_CHANGE_SEQ,
                        Type.STRING,
                        LAST_SEQ_NUM_DEFAULT,
                        Importance.LOW,
                        ResourceBundleUtil.get(MessageKey.CLOUDANT_LAST_SEQ_NUM_DOC),
                        DATABASE_GROUP,
                        order++,
                        Width.LONG,
                        ResourceBundleUtil.get(MessageKey.CLOUDANT_LAST_SEQ_NUM_DISP));
    }

    public SourceChangesConnectorConfig(Map<String, String> originals) {
        super(CONFIG_DEF, originals, false);
    }

    protected SourceChangesConnectorConfig(ConfigDef subclassConfigDef, Map<String, String> originals) {
        super(subclassConfigDef, originals);
    }

}
