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

import com.ibm.cloud.cloudant.kafka.utils.JavaCloudantUtil;
import com.ibm.cloud.cloudant.kafka.caching.CachedClientManager;
import com.ibm.cloud.cloudant.kafka.tasks.SinkConnectorConfig;
import com.ibm.cloud.cloudant.kafka.validators.ConfigValidator;
import com.ibm.cloud.cloudant.kafka.tasks.SinkTask;
import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SinkConnector extends org.apache.kafka.connect.sink.SinkConnector {

    private Map<String, String> configProperties;

    @Override
    public ConfigDef config() {
        return SinkConnectorConfig.CONFIG_DEF;
    }

    @Override
    public String version() {
        return JavaCloudantUtil.VERSION;
    }

    @Override
    public void start(Map<String, String> props) {
        configProperties = props;
    }

    @Override
    public Class<? extends Task> taskClass() {
        return SinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        return Collections.nCopies(maxTasks, new HashMap<>(configProperties));
    }

    @Override
    public void stop() {
        CachedClientManager.removeInstance(configProperties);
    }

    @Override
    public Config validate(Map<String, String> connectorConfigs) {
        ConfigValidator validator = new ConfigValidator(connectorConfigs, config());
        return validator.validate();
    }

}
