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
package com.clougence.clouddm.ds.kafka.dsconf;

import static com.clougence.clouddm.base.metadata.ui.form.UiUtils.fieldOptionDef;
import static com.clougence.clouddm.base.metadata.ui.form.UiUtils.strValueDef;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.clougence.clouddm.base.metadata.ds.DataSourceConfig;
import com.clougence.clouddm.base.metadata.ds.DsConfigGroup;
import com.clougence.clouddm.base.metadata.ds.SecurityType;
import com.clougence.clouddm.base.metadata.ds.SslMode;
import com.clougence.clouddm.base.metadata.ui.form.UiPanel;
import com.clougence.clouddm.base.metadata.ui.form.UiPanelField;
import com.clougence.clouddm.base.metadata.ui.form.UiPanelFieldType;
import com.clougence.clouddm.base.metadata.ui.form.value.ValueDef;
import com.clougence.clouddm.ds.kafka.i18n.KafkaConfigI18nKeys;
import com.clougence.clouddm.dsfamily.dsconf.AbstractDsConfigSpi;
import com.clougence.drivers.adapter.ConvertUtils;
import com.clougence.utils.StringUtils;

public class KafkaConfigSpi extends AbstractDsConfigSpi {

    @Override
    public String defaultPort() {
        return "9092";
    }

    @Override
    public Class<? extends DataSourceConfig> newConfig() {
        return KafkaConfig.class;
    }

    @Override
    public DataSourceConfig fillConfig(DataSourceConfig dsConfig, Map<String, String> defaultConfig) {
        KafkaConfig config = (KafkaConfig) dsConfig;
        Long connectTimeoutMs = ConvertUtils.toLong(defaultConfig.get(KafkaConfig.Fields.connectTimeoutMs), false);
        Integer soTimeoutSec = ConvertUtils.toInteger(defaultConfig.get(KafkaConfig.Fields.soTimeoutSec), false);
        String saslMechanism = defaultConfig.get(KafkaConfig.Fields.saslMechanism);
        if (StringUtils.isBlank(saslMechanism)) {
            saslMechanism = "PLAIN";
        }
        config.setSaslMechanism(saslMechanism);
        config.setConnectTimeoutMs(connectTimeoutMs == null ? 5000L : connectTimeoutMs);
        config.setSoTimeoutSec(soTimeoutSec == null ? 10 : soTimeoutSec);
        return dsConfig;
    }

    @Override
    public void customizePanels(Map<DsConfigGroup, UiPanel> panels) {
        UiPanel general = panels.get(DsConfigGroup.GENERAL);
        if (general == null) {
            return;
        }
        UiPanelField saslMechanism = general.findField(KafkaConfig.Fields.saslMechanism);
        if (saslMechanism == null) {
            return;
        }
        List<ValueDef> options = new ArrayList<>();
        options.add(fieldOptionDef(KafkaConfigI18nKeys.CONFIG_KAFKA_SASL_PLAIN, "PLAIN"));
        options.add(fieldOptionDef(KafkaConfigI18nKeys.CONFIG_KAFKA_SASL_SCRAM_256, "SCRAM-SHA-256"));
        options.add(fieldOptionDef(KafkaConfigI18nKeys.CONFIG_KAFKA_SASL_SCRAM_512, "SCRAM-SHA-512"));
        saslMechanism.setType(UiPanelFieldType.Options);
        saslMechanism.setOptions(options);
        if (saslMechanism.getDefaultValue() == null || saslMechanism.getDefaultValue().asValue() == null
            || StringUtils.isBlank(String.valueOf(saslMechanism.getDefaultValue().asValue()))) {
            saslMechanism.setDefaultValue(strValueDef("PLAIN"));
        }
    }

    @Override
    public List<SecurityType> securityTypes() {
        List<SecurityType> options = new ArrayList<>();
        options.add(SecurityType.NONE);
        options.add(SecurityType.USER_PASSWD);
        return options;
    }

    @Override
    public List<SslMode> sslModeSet() {
        return List.of(SslMode.TRUST, SslMode.CA, SslMode.TRUSTSTORE, SslMode.KEYSTORE_TRUSTSTORE, SslMode.CLIENT_CERT);
    }

    @Override
    public boolean supportTx() {
        return false;
    }

    @Override
    public boolean supportSSL() {
        return true;
    }

    @Override
    public boolean supportSSH() {
        return true;
    }
}
