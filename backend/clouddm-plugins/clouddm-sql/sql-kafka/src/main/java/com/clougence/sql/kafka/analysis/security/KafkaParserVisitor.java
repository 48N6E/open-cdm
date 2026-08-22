/*
 * Copyright 2026 杭州开云集致科技有限公司
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package com.clougence.sql.kafka.analysis.security;

import org.antlr.v4.runtime.Parser;
import org.antlr.v4.runtime.tree.RuleNode;

import com.clougence.clouddm.sdk.service.secrules.RuleQueryType;
import com.clougence.clouddm.sdk.service.secrules.SecQueryKind;
import com.clougence.clouddm.sdk.sql.parser.SplitQueryType;
import com.clougence.sql.kafka.analysis.KafkaAnalysisHelper;
import com.clougence.sql.kafka.analysis.security.builder.KafkaBuilderFactory;
import com.clougence.sql.kafka.analysis.security.domain.KafkaCmdDomain;
import com.clougence.sql.kafka.parser.KafkaSplitVisitor;
import com.clougence.sql.kafka.parser.antlr.KafkaParser;
import com.clougence.sql.kafka.parser.antlr.KafkaParserBaseVisitor;

public class KafkaParserVisitor extends KafkaParserBaseVisitor<Void> {

    private final KafkaBuilderFactory builder;

    public KafkaParserVisitor(KafkaBuilderFactory builder, Parser parser) {
        this.builder = builder;
    }

    private void buildDomain(KafkaParser.CommandContext ctx) {
        SplitQueryType splitQueryType = ctx.accept(KafkaSplitVisitor.INSTANCE);
        RuleQueryType queryType = KafkaAnalysisHelper.toRuleQueryType(splitQueryType);
        KafkaCmdDomain domain = new KafkaCmdDomain(KafkaAnalysisHelper.commandName(ctx));
        domain.setSqlType(queryType);
        domain.setAuditKind(queryType == RuleQueryType.READ ? SecQueryKind.QUERY : queryType.getAuditKind());
        if (ctx instanceof KafkaParser.CmdDescribeTopicInfoContext describeTopicInfo) {
            domain.setTopic(KafkaAnalysisHelper.topicName(describeTopicInfo.topicName()));
        } else if (ctx instanceof KafkaParser.CmdDescribeTopicContext describeTopic) {
            domain.setTopic(KafkaAnalysisHelper.topicName(describeTopic.topicName()));
        } else if (ctx instanceof KafkaParser.CmdDescribeGroupContext describeGroup) {
            domain.setTopic(KafkaAnalysisHelper.topicName(describeGroup.topicName()));
        } else if (ctx instanceof KafkaParser.CmdAlterTopicSetContext alterTopicSet) {
            domain.setTopic(KafkaAnalysisHelper.topicName(alterTopicSet.topicName()));
        } else if (ctx instanceof KafkaParser.CmdAlterTopicAddPartitionsContext alterTopicAddPartitions) {
            domain.setTopic(KafkaAnalysisHelper.topicName(alterTopicAddPartitions.topicName()));
        } else if (ctx instanceof KafkaParser.CmdAlterGroupResetOffsetContext alterGroup) {
            for (KafkaParser.ResetScopeContext scope : alterGroup.resetScope()) {
                if (scope.topicName() != null) {
                    domain.setTopic(KafkaAnalysisHelper.topicName(scope.topicName()));
                    break;
                }
            }
        } else if (ctx instanceof KafkaParser.CmdConsumeContext consume) {
            domain.setTopic(KafkaAnalysisHelper.topicName(consume.topicName()));
        }
        this.builder.addDomain(domain);
    }

    @Override
    public Void visitCmdShowTopics(KafkaParser.CmdShowTopicsContext ctx) {
        buildDomain(ctx);
        return null;
    }

    @Override
    public Void visitCmdShowGroups(KafkaParser.CmdShowGroupsContext ctx) {
        buildDomain(ctx);
        return null;
    }

    @Override
    public Void visitCmdDescribeCluster(KafkaParser.CmdDescribeClusterContext ctx) {
        buildDomain(ctx);
        return null;
    }

    @Override
    public Void visitCmdDescribeTopicInfo(KafkaParser.CmdDescribeTopicInfoContext ctx) {
        buildDomain(ctx);
        return null;
    }

    @Override
    public Void visitCmdDescribeTopic(KafkaParser.CmdDescribeTopicContext ctx) {
        buildDomain(ctx);
        return null;
    }

    @Override
    public Void visitCmdDescribeGroup(KafkaParser.CmdDescribeGroupContext ctx) {
        buildDomain(ctx);
        return null;
    }

    @Override
    public Void visitCmdDeleteGroup(KafkaParser.CmdDeleteGroupContext ctx) {
        buildDomain(ctx);
        return null;
    }

    @Override
    public Void visitCmdAlterTopicSet(KafkaParser.CmdAlterTopicSetContext ctx) {
        buildDomain(ctx);
        return null;
    }

    @Override
    public Void visitCmdAlterTopicAddPartitions(KafkaParser.CmdAlterTopicAddPartitionsContext ctx) {
        buildDomain(ctx);
        return null;
    }

    @Override
    public Void visitCmdAlterGroupResetOffset(KafkaParser.CmdAlterGroupResetOffsetContext ctx) {
        buildDomain(ctx);
        return null;
    }

    @Override
    public Void visitCmdConsume(KafkaParser.CmdConsumeContext ctx) {
        buildDomain(ctx);
        return null;
    }

    @Override
    public Void visitChildren(RuleNode node) {
        int n = node.getChildCount();
        for (int i = 0; i < n; i++) {
            node.getChild(i).accept(this);
        }
        return null;
    }
}
