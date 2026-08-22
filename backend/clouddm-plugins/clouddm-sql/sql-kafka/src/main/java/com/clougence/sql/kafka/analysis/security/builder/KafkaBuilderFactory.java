/*
 * Copyright 2026 杭州开云集致科技有限公司
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package com.clougence.sql.kafka.analysis.security.builder;

import com.clougence.clouddm.sdk.service.execute.MetaService;
import com.clougence.sql.common.analysis.secrules.builder.factory.SimpleBuilderFactory;

public class KafkaBuilderFactory extends SimpleBuilderFactory {

    public KafkaBuilderFactory(MetaService metaService) {
        super(metaService);
    }
}
