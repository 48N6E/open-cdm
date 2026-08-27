/*
 * Copyright 2026 杭州开云集致科技有限公司
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package com.clougence.sql.kafka;

import com.clougence.clouddm.sdk.DsPlugin;
import com.clougence.clouddm.sdk.DsPluginBinder;
import com.clougence.clouddm.sdk.Plugin;
import com.clougence.clouddm.sdk.service.execute.MetaService;
import com.clougence.sql.kafka.i18n.KafkaSqlI18nKeys;

@Plugin(name = "Kafka Commands", display = false)
public class KafkaSqlPlugin implements DsPlugin {

    @Override
    public void loadPlugin(DsPluginBinder dsPlugin) {
        dsPlugin.bindGlobalI18n(KafkaSqlI18nKeys.class);
        dsPlugin.addGlobalSpi(new KafkaSqlEngineSpi(dsPlugin.findGlobalService(MetaService.class)));
    }
}
