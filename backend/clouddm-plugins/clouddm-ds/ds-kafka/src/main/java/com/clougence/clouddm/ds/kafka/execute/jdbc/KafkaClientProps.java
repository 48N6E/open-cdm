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

import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.serialization.StringDeserializer;

import com.clougence.clouddm.base.metadata.ds.SslMode;
import com.clougence.utils.StringUtils;

class KafkaClientProps {

    static Properties adminProps(Map<String, String> dsConfig, String bootstrap) {
        Properties props = baseProps(dsConfig, bootstrap);
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        return props;
    }

    static Properties consumerProps(Map<String, String> dsConfig, String bootstrap) {
        Properties props = baseProps(dsConfig, bootstrap);
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "clouddm-" + UUID.randomUUID());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");
        return props;
    }

    private static Properties baseProps(Map<String, String> dsConfig, String bootstrap) {
        Properties props = new Properties();
        props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(CommonClientConfigs.CLIENT_ID_CONFIG, clientName(dsConfig));
        int connTimeoutMs = toInt(dsConfig.get(KafkaKeys.CONN_TIMEOUT), 5000);
        int soTimeoutMs = toInt(dsConfig.get(KafkaKeys.SO_TIMEOUT), 10000);
        props.put(CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG, String.valueOf(connTimeoutMs));
        props.put(CommonClientConfigs.DEFAULT_API_TIMEOUT_MS_CONFIG, String.valueOf(soTimeoutMs));

        SslMode sslMode = sslMode(dsConfig.get(KafkaKeys.SSL_MODE));
        boolean ssl = sslMode != SslMode.DISABLED;
        String username = StringUtils.trimToNull(dsConfig.get(KafkaKeys.USERNAME));
        String password = StringUtils.trimToNull(dsConfig.get(KafkaKeys.PASSWORD));
        boolean sasl = username != null || password != null;
        if (sasl && ssl) {
            props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
        } else if (sasl) {
            props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
        } else if (ssl) {
            props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL");
        } else {
            props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "PLAINTEXT");
        }
        if (sasl) {
            String mechanism = StringUtils.defaultIfBlank(dsConfig.get(KafkaKeys.SASL_MECHANISM), "PLAIN");
            props.put(SaslConfigs.SASL_MECHANISM, mechanism);
            props.put(SaslConfigs.SASL_JAAS_CONFIG, jaas(mechanism, username, password));
        }
        if (ssl) {
            applySsl(props, dsConfig, sslMode);
        }
        return props;
    }

    private static void applySsl(Properties props, Map<String, String> dsConfig, SslMode sslMode) {
        if (sslMode == SslMode.TRUST) {
            props.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "");
            return;
        }
        String truststore = StringUtils.trimToNull(dsConfig.get(KafkaKeys.SSL_CA_FILE));
        if (truststore != null) {
            props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, truststore);
            String trustPassword = dsConfig.get(KafkaKeys.SSL_CA_PASSWORD);
            if (StringUtils.isNotBlank(trustPassword)) {
                props.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, trustPassword);
            }
        }
        if (sslMode == SslMode.CLIENT_CERT || sslMode == SslMode.KEYSTORE_TRUSTSTORE) {
            String keystore = StringUtils.trimToNull(dsConfig.get(KafkaKeys.SSL_CLIENT_CERT_FILE));
            if (keystore == null) {
                keystore = StringUtils.trimToNull(dsConfig.get(KafkaKeys.SSL_CLIENT_KEY_FILE));
            }
            if (keystore != null) {
                props.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, keystore);
            }
            String keyPassword = dsConfig.get(KafkaKeys.SSL_CLIENT_KEY_PASSWORD);
            if (StringUtils.isNotBlank(keyPassword)) {
                props.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, keyPassword);
                props.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, keyPassword);
            }
        }
    }

    private static String jaas(String mechanism, String username, String password) {
        String user = username == null ? "" : username.replace("\"", "\\\"");
        String pass = password == null ? "" : password.replace("\"", "\\\"");
        if (mechanism != null && mechanism.toUpperCase().startsWith("SCRAM")) {
            return "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"" + user + "\" password=\"" + pass + "\";";
        }
        return "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"" + user + "\" password=\"" + pass + "\";";
    }

    private static String clientName(Map<String, String> dsConfig) {
        String name = StringUtils.trimToNull(dsConfig.get(KafkaKeys.CLIENT_NAME));
        if (name == null) {
            name = KafkaKeys.DEFAULT_CLIENT_NAME;
        } else {
            name = name.replace(" ", "-");
        }
        return name + "-" + UUID.randomUUID().toString().substring(0, 8);
    }

    private static SslMode sslMode(String value) {
        if (StringUtils.isBlank(value)) {
            return SslMode.DISABLED;
        }
        return SslMode.valueOf(value);
    }

    private static int toInt(String value, int defaultValue) {
        if (StringUtils.isBlank(value)) {
            return defaultValue;
        }
        return Integer.parseInt(value);
    }
}
