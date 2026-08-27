/*
 * Copyright 2026 杭州开云集致科技有限公司
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package com.clougence.sql.kafka.parser;

import org.antlr.v4.runtime.tree.RuleNode;

import com.clougence.clouddm.sdk.sql.parser.SplitQueryType;
import com.clougence.sql.kafka.parser.antlr.KafkaParser;
import com.clougence.sql.kafka.parser.antlr.KafkaParserBaseVisitor;

public class KafkaSplitVisitor extends KafkaParserBaseVisitor<SplitQueryType> {

    public static final KafkaSplitVisitor INSTANCE = new KafkaSplitVisitor();

    @Override
    public SplitQueryType visitCmdShowTopics(KafkaParser.CmdShowTopicsContext ctx) {
        return SplitQueryType.METADATA;
    }

    @Override
    public SplitQueryType visitCmdShowGroups(KafkaParser.CmdShowGroupsContext ctx) {
        return SplitQueryType.METADATA;
    }

    @Override
    public SplitQueryType visitCmdDescribeCluster(KafkaParser.CmdDescribeClusterContext ctx) {
        return SplitQueryType.METADATA;
    }

    @Override
    public SplitQueryType visitCmdDescribeTopicInfo(KafkaParser.CmdDescribeTopicInfoContext ctx) {
        return SplitQueryType.METADATA;
    }

    @Override
    public SplitQueryType visitCmdDescribeTopic(KafkaParser.CmdDescribeTopicContext ctx) {
        return SplitQueryType.METADATA;
    }

    @Override
    public SplitQueryType visitCmdDescribeGroup(KafkaParser.CmdDescribeGroupContext ctx) {
        return SplitQueryType.METADATA;
    }

    @Override
    public SplitQueryType visitCmdDeleteGroup(KafkaParser.CmdDeleteGroupContext ctx) {
        return SplitQueryType.DELETE;
    }

    @Override
    public SplitQueryType visitCmdAlterTopicSet(KafkaParser.CmdAlterTopicSetContext ctx) {
        return SplitQueryType.UPDATE;
    }

    @Override
    public SplitQueryType visitCmdAlterTopicAddPartitions(KafkaParser.CmdAlterTopicAddPartitionsContext ctx) {
        return SplitQueryType.ALTER_TABLE;
    }

    @Override
    public SplitQueryType visitCmdAlterGroupResetOffset(KafkaParser.CmdAlterGroupResetOffsetContext ctx) {
        return SplitQueryType.UPDATE;
    }

    @Override
    public SplitQueryType visitCmdConsume(KafkaParser.CmdConsumeContext ctx) {
        return SplitQueryType.SELECT;
    }

    @Override
    protected SplitQueryType aggregateResult(SplitQueryType aggregate, SplitQueryType nextResult) {
        if (nextResult != null) {
            return nextResult;
        }
        return aggregate;
    }

    @Override
    public SplitQueryType visitChildren(RuleNode node) {
        SplitQueryType result = SplitQueryType.UNKNOWN;
        int n = node.getChildCount();
        for (int i = 0; i < n; i++) {
            SplitQueryType childResult = node.getChild(i).accept(this);
            result = aggregateResult(result, childResult);
        }
        return result;
    }
}
