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

package com.ibm.cloud.cloudant.kafka;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.ibm.cloud.cloudant.kafka.caching.CachedClientManager;
import com.ibm.cloud.cloudant.kafka.tasks.ConnectorConfig;
import com.ibm.cloud.cloudant.kafka.utils.JavaCloudantUtil;
import com.ibm.cloud.cloudant.kafka.validators.ConfigValidator;

public abstract class AbstractSourceConnector<T extends Task> extends SourceConnector {

    protected final Logger LOG = LoggerFactory.getLogger(this.getClass());
    protected Map<String, String> configProperties;
    protected ConnectorConfig config;
    protected Class<T> taskClass;
    protected ConfigDef connectorConfigDef;

    AbstractSourceConnector(Class<T> taskClass, ConfigDef connectorConfigDef) {
        this.taskClass = taskClass;
        this.connectorConfigDef = connectorConfigDef;
    }

    @Override
    public String version() {
        return JavaCloudantUtil.VERSION;
    }

    @Override
    public void start(Map<String, String> props) {
        this.configProperties = props;
        this.config = new ConnectorConfig(config(), this.configProperties);
    }

    @Override
    public Class<T> taskClass() {
        return taskClass;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        return Collections.nCopies(maxTasks, new HashMap<>(configProperties));
    }

    @Override
    public void stop() {
        CachedClientManager.removeInstance(this.configProperties);
    }

    @Override
    public ConfigDef config() {
        return this.connectorConfigDef;
    }

    @Override
    public Config validate(Map<String, String> connectorConfigs) {
        ConfigValidator validator = new ConfigValidator(connectorConfigs, config());
        return validator.validate();
    }
}
