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

import java.util.Map;

public class SinkConnectorConfig extends ConnectorConfig {

    public static final ConfigDef CONFIG_DEF = baseConfigDef();

    public static ConfigDef baseConfigDef() {
        return new ConfigDef(ConnectorConfig.CONFIG_DEF)
                // batch size
                .define(InterfaceConst.BATCH_SIZE,
                        ConfigDef.Type.INT,
                        InterfaceConst.DEFAULT_BATCH_SIZE_SINK,
                        ConfigDef.Range.between(InterfaceConst.BATCH_SIZE_MIN_SINK, InterfaceConst.BATCH_SIZE_MAX_SINK),
                        ConfigDef.Importance.MEDIUM,
                        ResourceBundleUtil.get(MessageKey.CLOUDANT_BATCH_SIZE_SINK_DOC),
                        DATABASE_GROUP,
                        1,
                        ConfigDef.Width.SHORT,
                        ResourceBundleUtil.get(MessageKey.CLOUDANT_BATCH_SIZE_DISP));
    }

    protected SinkConnectorConfig(ConfigDef subclassConfigDef, Map<String, String> originals) {
        super(subclassConfigDef, originals);
    }

}
