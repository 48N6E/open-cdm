/*
 * Copyright 2026 杭州开云集致科技有限公司
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package com.clougence.sql.kafka.analysis.security;

import java.io.Reader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import org.antlr.v4.runtime.Parser;
import org.antlr.v4.runtime.tree.AbstractParseTreeVisitor;

import com.clougence.clouddm.base.metadata.ds.DataSourceType;
import com.clougence.clouddm.sdk.service.execute.MetaService;
import com.clougence.clouddm.sdk.service.secrules.RuleDomain;
import com.clougence.clouddm.sdk.sql.analysis.security.ContextInfo;
import com.clougence.clouddm.sdk.sql.analysis.security.SecDomainResolveSpi;
import com.clougence.clouddm.sdk.sql.parser.SplitScript;
import com.clougence.dslpaser.antlr.DslHelper;
import com.clougence.dslpaser.antlr.DslProvider;
import com.clougence.dslpaser.ast.location.CodeLocation;
import com.clougence.dslpaser.parse.AstSplitScript;
import com.clougence.sql.kafka.analysis.security.builder.KafkaBuilderFactory;
import com.clougence.sql.kafka.parser.KafkaDslProvider;
import com.clougence.sql.kafka.parser.KafkaSplitAnalysisSpi;

public class KafkaSecDomainResolveSpi implements SecDomainResolveSpi {

    private final MetaService metaService;

    public KafkaSecDomainResolveSpi(MetaService metaService) {
        this.metaService = metaService;
    }

    protected DslProvider dslProvider() {
        return KafkaDslProvider.INSTANCE;
    }

    protected AbstractParseTreeVisitor<Void> parserVisitor(KafkaBuilderFactory domainBuilder, Parser parser) {
        return new KafkaParserVisitor(domainBuilder, parser);
    }

    @Override
    public Stream<RuleDomain> resolveDomainStream(DataSourceType dsType, Reader queryReader, int baseLine, int baseColumn, ContextInfo ctxInfo) {
        var scripts = new KafkaSplitAnalysisSpi().splitScriptStream(queryReader, List.of(), baseLine, baseColumn);
        return scripts.flatMap(script -> {
            StringReader reader = new StringReader(script.getScript());
            int codeLine = script.getBodyStartCodeLine();
            int codeColumn = script.getBodyStartCodeColumn();
            return resolveStatement(dsType, reader, codeLine, codeColumn).stream();
        }).onClose(scripts::close);
    }

    private List<RuleDomain> resolveStatement(DataSourceType dsType, Reader queryReader, int baseLine, int baseColumn) {
        CodeLocation dslBase = new CodeLocation(baseLine, baseColumn);
        List<RuleDomain> domainList = new ArrayList<>();

        List<AstSplitScript> scripts = DslHelper.splitDsl(dslProvider(), queryReader, dslBase);
        for (AstSplitScript s : scripts) {
            SplitScript ss = new SplitScript();
            ss.setScript(s.getScript());
            ss.setBodyStartCodeLine(s.getBodyStartCodeLine());
            ss.setBodyEndCodeLine(s.getEndCodeLine());
            ss.setBodyStartCodeColumn(s.getBodyStartCodeColumn());
            ss.setBodyEndCodeColumn(s.getEndCodeColumn());

            KafkaBuilderFactory builder = new KafkaBuilderFactory(this.metaService);
            try (StringReader reader = new StringReader(s.getScript())) {
                DslHelper.doVisitor(dslProvider(), reader, (lexer, parser) -> this.parserVisitor(builder, parser));
            }
            List<RuleDomain> build = builder.build();
            for (RuleDomain domain : build) {
                domain.setDsType(dsType);
                domain.setSplitScript(ss);
                domainList.add(domain);
            }
        }
        return domainList;
    }
}
