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
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;

import java.util.Map;

public class CloudantSourceTaskConfig extends CloudantSourceConnectorConfig {

    // Expand this ConfigDef with task specific parameters
    static org.apache.kafka.common.config.ConfigDef config = new ConfigDef(baseConfigDef())
            .define(InterfaceConst.TASK_NUMBER,
                    Type.INT, InterfaceConst.DEFAULT_TASK_NUMBER, Importance.HIGH, InterfaceConst.TASK_NUMBER)
            .define(InterfaceConst.TASKS_MAX,
                    Type.INT, InterfaceConst.DEFAULT_TASKS_MAX, Importance.LOW, InterfaceConst.TASKS_MAX);

    public CloudantSourceTaskConfig(Map<String, String> originals) {
        super(config, originals);
    }

}
