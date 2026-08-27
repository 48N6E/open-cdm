/*
 * Copyright 2026 杭州开云集致科技有限公司
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package com.clougence.sql.kafka.parser.ast;

import java.io.IOException;

import com.clougence.dslpaser.ast.location.BlockLocation;
import com.clougence.dslpaser.ast.visitor.Visitor;
import com.clougence.dslpaser.ast.visitor.VisitorTree;
import com.clougence.dslpaser.foramt.FmtWriter;

public abstract class AbstractKafkaAst extends BlockLocation implements VisitorTree {

    @Override
    public void accept(Visitor visitor) {
    }

    public void doFormat(FmtWriter writer) throws IOException {
    }
}
