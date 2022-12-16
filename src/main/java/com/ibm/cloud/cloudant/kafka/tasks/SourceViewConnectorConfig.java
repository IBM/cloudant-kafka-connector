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

package com.ibm.cloud.cloudant.kafka.tasks;

import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;
import com.ibm.cloud.cloudant.kafka.utils.InterfaceConst;
import com.ibm.cloud.cloudant.kafka.utils.MessageKey;
import com.ibm.cloud.cloudant.kafka.utils.ResourceBundleUtil;

public class SourceViewConnectorConfig extends ConnectorConfig {
 
    public static final ConfigDef CONFIG_DEF = baseConfigDef();

    public static ConfigDef baseConfigDef() {

        int order = 100; // pick a high number so these will come after those from base config def

        return new ConfigDef(ConnectorConfig.CONFIG_DEF)
        // TODO add validator for no _design/ prefix
        .define(InterfaceConst.DESIGN_DOC,
                Type.STRING,
                ConfigDef.NO_DEFAULT_VALUE,
                Importance.HIGH,
                ResourceBundleUtil.get(MessageKey.CLOUDANT_DESIGN_DOC_DOC),
                DATABASE_GROUP,
                order++,
                Width.MEDIUM,
                ResourceBundleUtil.get(MessageKey.CLOUDANT_DESIGN_DOC_DISP))
        .define(InterfaceConst.VIEW_NAME,
                Type.STRING,
                ConfigDef.NO_DEFAULT_VALUE,
                Importance.HIGH,
                ResourceBundleUtil.get(MessageKey.CLOUDANT_VIEW_NAME_DOC),
                DATABASE_GROUP,
                order++,
                Width.MEDIUM,
                ResourceBundleUtil.get(MessageKey.CLOUDANT_VIEW_NAME_DISP));
    }

    public SourceViewConnectorConfig(Map<String, String> originals) {
        super(CONFIG_DEF, originals, false);
    }

    protected SourceViewConnectorConfig(ConfigDef subclassConfigDef, Map<String, String> originals) {
        super(subclassConfigDef, originals);
    }
    
}
