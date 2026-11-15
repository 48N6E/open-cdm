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
package com.clougence.clouddm.dsfamily.mysql.execute.dsfactory;

import java.util.Properties;

import com.clougence.clouddm.base.metadata.ds.DataSourceConfig;
import com.clougence.clouddm.base.metadata.ds.SslMode;
import com.clougence.utils.StringUtils;

/**
 * Maps CloudDM {@link SslMode} / certificate files onto MySQL Connector/J SSL properties.
 * Shared by MySQL, Doris and SelectDB which all use the MySQL JDBC driver family.
 */
public final class MySqlJdbcSslProperties {

    private MySqlJdbcSslProperties(){
    }

    public static void apply(Properties properties, DataSourceConfig config) {
        properties.setProperty("sslMode", jdbcSslMode(config.getSslMode()));
        if (config.getSslMode() == null) {
            return;
        }
        switch (config.getSslMode()) {
            case CA, TRUSTSTORE -> applyTrustStore(properties, config, safeStr(config.getSslCaPassword()));
            case KEYSTORE_TRUSTSTORE -> {
                applyTrustStore(properties, config, safeStr(config.getSslCaPassword()));
                applyClientKeyStore(properties, config);
            }
            case CLIENT_CERT -> {
                applyTrustStore(properties, config, "");
                String clientKeyStoreFilePath = StringUtils.isNotBlank(config.getSslClientKeyFilePath()) ? config.getSslClientKeyFilePath() : config.getSslClientCertFilePath();
                if (StringUtils.isBlank(clientKeyStoreFilePath)) {
                    break;
                }
                properties.setProperty("clientCertificateKeyStoreUrl", "file:" + clientKeyStoreFilePath);
                String clientKeyStoreFormat = StringUtils.isNotBlank(config.getSslClientKeyFilePath()) ? config.getSslClientKeyFileFormat() : config.getSslClientCertFileFormat();
                properties.setProperty("clientCertificateKeyStoreType", keyStoreType(clientKeyStoreFormat, "client certificate"));
                properties.setProperty("fallbackToSystemKeyStore", "false");
                if (StringUtils.isNotBlank(config.getSslClientKeyPassword())) {
                    properties.setProperty("clientCertificateKeyStorePassword", config.getSslClientKeyPassword());
                }
            }
            default -> {
            }
        }
    }

    public static String jdbcSslMode(SslMode sslMode) {
        if (sslMode == null) {
            return "DISABLED";
        }
        return switch (sslMode) {
            case TRUST -> "REQUIRED";
            case CA, TRUSTSTORE -> "VERIFY_CA";
            case KEYSTORE_TRUSTSTORE, CLIENT_CERT -> "VERIFY_IDENTITY";
            default -> "DISABLED";
        };
    }

    private static void applyTrustStore(Properties properties, DataSourceConfig config, String password) {
        if (StringUtils.isBlank(config.getSslCaFilePath())) {
            return;
        }
        properties.setProperty("trustCertificateKeyStoreUrl", "file:" + config.getSslCaFilePath());
        properties.setProperty("trustCertificateKeyStoreType", keyStoreType(config.getSslCaFileFormat(), "CA"));
        properties.setProperty("trustCertificateKeyStorePassword", password);
        properties.setProperty("fallbackToSystemTrustStore", "false");
    }

    private static void applyClientKeyStore(Properties properties, DataSourceConfig config) {
        if (StringUtils.isBlank(config.getSslClientCertFilePath())) {
            return;
        }
        properties.setProperty("clientCertificateKeyStoreUrl", "file:" + config.getSslClientCertFilePath());
        properties.setProperty("clientCertificateKeyStoreType", keyStoreType(config.getSslClientCertFileFormat(), "client certificate"));
        properties.setProperty("clientCertificateKeyStorePassword", safeStr(config.getSslClientKeyPassword()));
        properties.setProperty("fallbackToSystemKeyStore", "false");
    }

    private static String keyStoreType(String format, String usage) {
        if (StringUtils.isBlank(format)) {
            return "PKCS12";
        }
        return switch (format.toLowerCase()) {
            case "p12", "pfx" -> "PKCS12";
            case "jks" -> "JKS";
            default -> throw new IllegalArgumentException("MySQL SSL " + usage + " file must be a KeyStore");
        };
    }

    private static String safeStr(String value) {
        return value == null ? "" : value;
    }
}
