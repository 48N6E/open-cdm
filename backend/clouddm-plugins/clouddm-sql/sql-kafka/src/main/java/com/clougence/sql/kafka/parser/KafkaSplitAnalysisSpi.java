/*
 * Copyright 2026 杭州开云集致科技有限公司
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package com.clougence.sql.kafka.parser;

import org.antlr.v4.runtime.Parser;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.tree.AbstractParseTreeVisitor;

import com.clougence.clouddm.sdk.sql.parser.SplitQueryType;
import com.clougence.dslpaser.antlr.DslProvider;
import com.clougence.dslpaser.parse.AntlrStatementParser;
import com.clougence.sql.common.parser.AbstractSplitAnalysisSpi;
import com.clougence.sql.kafka.parser.antlr.KafkaParser;

public class KafkaSplitAnalysisSpi extends AbstractSplitAnalysisSpi {

    @Override
    protected DslProvider dslProvider() {
        return KafkaDslProvider.INSTANCE;
    }

    @Override
    protected AbstractParseTreeVisitor<SplitQueryType> splitVisitor() {
        return KafkaSplitVisitor.INSTANCE;
    }

    @Override
    protected void parseRoot(Parser parser) {
        ((KafkaParser) parser).rootInstSet();
    }

    @Override
    protected boolean isStatementContext(ParserRuleContext context) {
        return context instanceof KafkaParser.CmdInstContext && context.getStart() != null && context.getStop() != null;
    }

    @Override
    protected AntlrStatementParser statementParser() {
        return new KafkaAntlrStatementParser();
    }
}
