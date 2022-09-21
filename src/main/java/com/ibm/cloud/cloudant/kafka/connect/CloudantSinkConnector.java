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
import com.ibm.cloud.cloudant.kafka.common.utils.JavaCloudantUtil;
import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CloudantSinkConnector extends SinkConnector {

    private static Logger LOG = LoggerFactory.getLogger(CloudantSinkConnector.class);

    private Map<String, String> configProperties;

    @Override
    public ConfigDef config() {
        return CloudantSinkConnectorConfig.CONFIG_DEF;
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
        return CloudantSinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        List<Map<String, String>> taskConfigs = new ArrayList<>(maxTasks);

        for (int i = 0; i < maxTasks; i++) {
            Map<String, String> taskProps = new HashMap<>(configProperties);
            // add task specific properties here (if any)
            taskProps.put(InterfaceConst.TASK_NUMBER, String.valueOf(i));

            taskConfigs.add(taskProps);
        }
        return taskConfigs;
    }

    @Override
    public void stop() {
        CachedClientManager.removeInstance(configProperties);
    }

    @Override
    public Config validate(Map<String, String> connectorConfigs) {
        CloudantConfigValidator validator = new CloudantConfigValidator(connectorConfigs, config());
        return validator.validate();
    }

}
