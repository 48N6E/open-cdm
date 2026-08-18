/*
 * Copyright 2026 杭州开云集致科技有限公司
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.clougence.clouddm.ds.kafka.execute.dsfactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

import com.clougence.clouddm.ds.kafka.execute.jdbc.KafkaKeys;
import com.clougence.drivers.DsConfigKeys;
import com.clougence.drivers.DsFactory;
import com.clougence.drivers.DsObject;
import com.clougence.drivers.adapter.JdbcDriver;
import com.clougence.utils.StringUtils;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class KafkaJdbcDsFactory implements DsFactory<Connection> {

    @Override
    public DsObject<Connection> create(Properties dsConfig) throws SQLException {
        Properties props = new Properties();
        props.putAll(dsConfig);
        for (DsConfigKeys confKey : DsConfigKeys.values()) {
            props.remove(confKey.getConfigKey());
        }

        String id = dsConfig.getProperty(DsConfigKeys.ID.getConfigKey());
        String username = dsConfig.getProperty(DsConfigKeys.USER.getConfigKey());
        String password = dsConfig.getProperty(DsConfigKeys.PASSWORD.getConfigKey());
        String connTimeoutMs = dsConfig.getProperty(DsConfigKeys.CONNECT_TIMEOUT_MS.getConfigKey());
        String soTimeoutSec = dsConfig.getProperty(DsConfigKeys.SO_TIMEOUT_SEC.getConfigKey());
        String clientName = dsConfig.getProperty(DsConfigKeys.CLIENT_NAME.getConfigKey());
        if (StringUtils.isNotBlank(username)) {
            props.put(KafkaKeys.USERNAME, username);
        }
        if (StringUtils.isNotBlank(password)) {
            props.put(KafkaKeys.PASSWORD, password);
        }
        if (StringUtils.isNotBlank(clientName)) {
            props.put(KafkaKeys.CLIENT_NAME, clientName.replace(" ", "-"));
        }
        if (StringUtils.isNotBlank(connTimeoutMs)) {
            props.put(KafkaKeys.CONN_TIMEOUT, connTimeoutMs);
        }
        if (StringUtils.isNotBlank(soTimeoutSec)) {
            props.put(KafkaKeys.SO_TIMEOUT, String.valueOf(Long.parseLong(soTimeoutSec) * 1000));
        }

        String jdbcUrl = buildJdbcUrl(dsConfig);
        try {
            Connection connection = new JdbcDriver().connect(jdbcUrl, props);
            return new DsObject<>(dsConfig, connection, this);
        } catch (Exception e) {
            log.error("create Kafka connection failed, instanceId=" + id + ", jdbcUrl=" + jdbcUrl, e);
            throw e instanceof SQLException ? (SQLException) e : new SQLException(e);
        }
    }

    protected String buildJdbcUrl(Properties dsConfig) {
        String jdbcUrl = dsConfig.getProperty(DsConfigKeys.CUSTOM_URL.getConfigKey());
        if (StringUtils.isNotBlank(jdbcUrl)) {
            return jdbcUrl;
        }
        String host = dsConfig.getProperty(DsConfigKeys.HOST.getConfigKey());
        return KafkaKeys.START_URL + host;
    }
}
