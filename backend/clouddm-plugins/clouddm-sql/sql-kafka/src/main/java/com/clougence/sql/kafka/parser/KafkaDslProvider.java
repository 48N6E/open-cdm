/*
 * Copyright 2026 杭州开云集致科技有限公司
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package com.clougence.sql.kafka.parser;

import java.util.ArrayList;
import java.util.List;

import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.tree.AbstractParseTreeVisitor;
import org.antlr.v4.runtime.tree.ParseTree;

import com.clougence.dslpaser.antlr.DslProvider;
import com.clougence.dslpaser.ast.StatementSet;
import com.clougence.dslpaser.parse.AntlrStatementParser;
import com.clougence.dslpaser.parse.AstSplitScript;
import com.clougence.sql.kafka.KafkaSqlEngineSpi;
import com.clougence.sql.kafka.parser.antlr.KafkaLexer;
import com.clougence.sql.kafka.parser.antlr.KafkaParser;
import com.clougence.sql.kafka.parser.ast.KafkaCmdSet;

public class KafkaDslProvider implements DslProvider {

    public static final DslProvider INSTANCE = new KafkaDslProvider();
    private final AntlrStatementParser treeParser = new KafkaAntlrStatementParser();

    @Override
    public String[] getDslName() {
        return new String[] { KafkaSqlEngineSpi.NAME };
    }

    @Override
    public Lexer createLexer(CharStream charStream) {
        return new KafkaLexer(charStream);
    }

    @Override
    public Parser createParser(Lexer lexer) {
        return new KafkaParser(new CommonTokenStream(lexer));
    }

    @Override
    public StatementSet doParser(Lexer lexer, Parser parser) {
        return new KafkaCmdSet();
    }

    @Override
    public List<AstSplitScript> doSplit(Lexer lexer, Parser parser) {
        TokenStream tokenStream = parser.getTokenStream();
        List<ParseTree> astList = this.treeParser.statementList(lexer, parser);

        List<AstSplitScript> result = new ArrayList<>();
        ParseTree lastTree = null;
        for (ParseTree parseTree : astList) {
            ParserRuleContext context = (ParserRuleContext) parseTree;
            Token startToken = context.getStart();
            Token stopToken = context.getStop();

            result.add(AstSplitScript.builder()
                .script(this.treeParser.getTextKeepComment(tokenStream, lastTree, startToken, stopToken))
                .astTree(parseTree)
                .parser(parser)
                .lexer(lexer)
                .bodyStartCodeLine(startToken.getLine())
                .bodyStartCodeColumn(startToken.getCharPositionInLine())
                .build());
            lastTree = parseTree;
        }
        return result;
    }

    @Override
    public void doVisitor(Lexer lexer, Parser parser, AbstractParseTreeVisitor<?> visitor) {
        List<ParseTree> astList = this.treeParser.statementList(lexer, parser);
        for (ParseTree astTree : astList) {
            visitor.visit(astTree);
        }
    }
}
