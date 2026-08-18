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
package com.clougence.clouddm.ds.kafka.execute.jdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.admin.AdminClient;

import com.clougence.drivers.adapter.AdapterFactory;
import com.clougence.drivers.adapter.AdapterTypeSupport;
import com.clougence.drivers.adapter.TypeSupport;
import com.clougence.utils.StringUtils;
import com.clougence.utils.ref.LinkedCaseInsensitiveMap;

public class KafkaConnectionFactory implements AdapterFactory {

    @Override
    public String getAdapterName() { return KafkaKeys.ADAPTER_NAME_VALUE; }

    @Override
    public String[] getPropertyNames() {
        return new String[] { KafkaKeys.SERVER, KafkaKeys.ADAPTER_NAME, KafkaKeys.CONN_TIMEOUT, KafkaKeys.SO_TIMEOUT, KafkaKeys.USERNAME, KafkaKeys.PASSWORD,
                              KafkaKeys.CLIENT_NAME, KafkaKeys.SASL_MECHANISM, KafkaKeys.SSL_MODE, KafkaKeys.SSL_CA_FILE, KafkaKeys.SSL_CA_PASSWORD,
                              KafkaKeys.SSL_CLIENT_CERT_FILE, KafkaKeys.SSL_CLIENT_KEY_FILE, KafkaKeys.SSL_CLIENT_KEY_PASSWORD };
    }

    @Override
    public TypeSupport createTypeSupport(Properties properties) {
        return new AdapterTypeSupport(properties);
    }

    @Override
    public KafkaConnection createConnection(Connection owner, String jdbcUrl, Properties props) throws SQLException {
        Map<String, String> caseProps = new LinkedCaseInsensitiveMap<>();
        props.forEach((k, v) -> caseProps.put((String) k, v == null ? "" : String.valueOf(v)));
        String bootstrap = normalizeBootstrap(caseProps.get(KafkaKeys.SERVER));
        Properties adminProps = KafkaClientProps.adminProps(caseProps, bootstrap);
        AdminClient admin = AdminClient.create(adminProps);
        try {
            int timeoutMs = StringUtils.isBlank(caseProps.get(KafkaKeys.SO_TIMEOUT)) ? 10000 : Integer.parseInt(caseProps.get(KafkaKeys.SO_TIMEOUT));
            admin.describeCluster().clusterId().get(timeoutMs, TimeUnit.MILLISECONDS);
            return new KafkaConnection(owner, admin, caseProps, bootstrap, jdbcUrl);
        } catch (Exception e) {
            admin.close();
            throw new SQLException("create Kafka connection failed: " + e.getMessage(), e);
        }
    }

    static String normalizeBootstrap(String host) {
        if (StringUtils.isBlank(host)) {
            throw new IllegalArgumentException("Kafka bootstrap servers is required.");
        }
        List<String> servers = new ArrayList<>();
        for (String part : host.split(",")) {
            String item = part.trim();
            if (item.isEmpty()) {
                continue;
            }
            if (!item.contains(":")) {
                item = item + ":9092";
            }
            servers.add(item);
        }
        if (servers.isEmpty()) {
            throw new IllegalArgumentException("Kafka bootstrap servers is required.");
        }
        return StringUtils.join(servers, ",");
    }
}
