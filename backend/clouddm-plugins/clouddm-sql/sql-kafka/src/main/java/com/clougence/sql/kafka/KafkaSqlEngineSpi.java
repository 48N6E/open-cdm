/*
 * Copyright 2026 杭州开云集致科技有限公司
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package com.clougence.sql.kafka;

import com.clougence.clouddm.sdk.service.execute.MetaService;
import com.clougence.clouddm.sdk.sql.SqlEngineSpi;
import com.clougence.clouddm.sdk.sql.SqlParserParameters;
import com.clougence.clouddm.sdk.sql.analysis.behavior.BehaviorAnalysisSpi;
import com.clougence.clouddm.sdk.sql.analysis.lineage.LineageAnalysisSpi;
import com.clougence.clouddm.sdk.sql.analysis.security.SecDomainResolveSpi;
import com.clougence.clouddm.sdk.sql.editor.rewrite.RewriteSpi;
import com.clougence.clouddm.sdk.sql.parser.SplitAnalysisSpi;
import com.clougence.dslpaser.antlr.DslProvider;
import com.clougence.sql.kafka.analysis.behavior.KafkaBehaviorAnalysisSpi;
import com.clougence.sql.kafka.analysis.security.KafkaSecDomainResolveSpi;
import com.clougence.sql.kafka.parser.KafkaDslProvider;
import com.clougence.sql.kafka.parser.KafkaSplitAnalysisSpi;

public class KafkaSqlEngineSpi implements SqlEngineSpi {

    public static final String NAME = "Kafka Commands";

    private final SplitAnalysisSpi splitAnalysisSpi;
    private final SecDomainResolveSpi secDomainResolveSpi;
    private final BehaviorAnalysisSpi behaviorAnalysisSpi;

    public KafkaSqlEngineSpi(MetaService metaService) {
        this.splitAnalysisSpi = new KafkaSplitAnalysisSpi();
        this.secDomainResolveSpi = new KafkaSecDomainResolveSpi(metaService);
        this.behaviorAnalysisSpi = new KafkaBehaviorAnalysisSpi();
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public DslProvider dslProvider(SqlParserParameters parameters) {
        return KafkaDslProvider.INSTANCE;
    }

    @Override
    public SplitAnalysisSpi splitAnalysisSpi(SqlParserParameters parameters) {
        return splitAnalysisSpi;
    }

    @Override
    public SecDomainResolveSpi secDomainResolveSpi(SqlParserParameters parameters) {
        return secDomainResolveSpi;
    }

    @Override
    public BehaviorAnalysisSpi behaviorAnalysisSpi(SqlParserParameters parameters) {
        return behaviorAnalysisSpi;
    }

    @Override
    public LineageAnalysisSpi lineageAnalysisSpi(SqlParserParameters parameters) {
        return LineageAnalysisSpi.EMPTY;
    }

    @Override
    public RewriteSpi rewriteSpi(SqlParserParameters parameters) {
        return null;
    }
}
