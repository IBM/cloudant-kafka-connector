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
package com.ibm.cloud.cloudant.kafka;

import com.ibm.cloud.cloudant.kafka.tasks.SourceChangesConnectorConfig;
import com.ibm.cloud.cloudant.kafka.tasks.SourceChangesTask;

import java.util.List;
import java.util.Map;

public class SourceChangesConnector extends AbstractSourceConnector<SourceChangesTask> {

    public SourceChangesConnector() {
        super(SourceChangesTask.class, SourceChangesConnectorConfig.CONFIG_DEF);
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        if (maxTasks > 1) {
            LOG.warn("tasks.max requested was {}, but only 1 task supported", maxTasks);
        }
        // Should this have been 1??
        return super.taskConfigs(maxTasks);
    }
}
