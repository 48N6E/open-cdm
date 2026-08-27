/*
 * Copyright 2026 杭州开云集致科技有限公司
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package com.clougence.sql.kafka.parser.ast;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.clougence.dslpaser.ast.Statement;
import com.clougence.dslpaser.ast.StatementSet;
import com.clougence.dslpaser.foramt.FmtWriter;

import lombok.Getter;

@Getter
public class KafkaCmdSet extends AbstractKafkaAst implements StatementSet {

    private final List<Statement> statementList = new ArrayList<>();

    @Override
    public List<Statement> getStatements() {
        return this.statementList;
    }

    @Override
    public void doFormat(FmtWriter writer) throws IOException {
    }
}
