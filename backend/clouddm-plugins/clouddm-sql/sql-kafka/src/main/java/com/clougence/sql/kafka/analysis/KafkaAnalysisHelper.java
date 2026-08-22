/*
 * Copyright 2026 杭州开云集致科技有限公司
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package com.clougence.sql.kafka.analysis;

import com.clougence.clouddm.sdk.service.secrules.RuleQueryType;
import com.clougence.clouddm.sdk.sql.parser.SplitQueryType;
import com.clougence.sql.kafka.parser.antlr.KafkaParser;

public final class KafkaAnalysisHelper {

    private KafkaAnalysisHelper() {
    }

    public static RuleQueryType toRuleQueryType(SplitQueryType type) {
        return switch (type) {
            case SELECT, METADATA, PERFORMANCE, LOG_READ -> RuleQueryType.READ;
            case INSERT -> RuleQueryType.INSERT;
            case UPDATE, ALTER_TABLE, MERGE -> RuleQueryType.UPDATE;
            case DELETE -> RuleQueryType.DELETE;
            default -> RuleQueryType.UNKNOWN;
        };
    }

    public static String topicName(KafkaParser.TopicNameContext ctx) {
        return unwrap(ctx == null ? null : ctx.getText());
    }

    public static String groupName(KafkaParser.GroupNameContext ctx) {
        return unwrap(ctx == null ? null : ctx.getText());
    }

    private static String unwrap(String text) {
        if (text == null) {
            return null;
        }
        if (text.length() >= 2) {
            char first = text.charAt(0);
            char last = text.charAt(text.length() - 1);
            if ((first == '\'' && last == '\'') || (first == '"' && last == '"')) {
                return text.substring(1, text.length() - 1);
            }
        }
        return text;
    }

    public static String commandName(KafkaParser.CommandContext ctx) {
        if (ctx instanceof KafkaParser.CmdShowTopicsContext) {
            return "SHOW TOPICS";
        }
        if (ctx instanceof KafkaParser.CmdShowGroupsContext) {
            return "SHOW GROUPS";
        }
        if (ctx instanceof KafkaParser.CmdDescribeClusterContext) {
            return "DESCRIBE CLUSTER";
        }
        if (ctx instanceof KafkaParser.CmdDescribeTopicInfoContext) {
            return "DESCRIBE TOPIC INFO";
        }
        if (ctx instanceof KafkaParser.CmdDescribeTopicContext) {
            return "DESCRIBE TOPIC";
        }
        if (ctx instanceof KafkaParser.CmdDescribeGroupContext) {
            return "DESCRIBE GROUP";
        }
        if (ctx instanceof KafkaParser.CmdDeleteGroupContext) {
            return "DELETE GROUP";
        }
        if (ctx instanceof KafkaParser.CmdAlterTopicSetContext) {
            return "ALTER TOPIC SET";
        }
        if (ctx instanceof KafkaParser.CmdAlterTopicAddPartitionsContext) {
            return "ALTER TOPIC ADD PARTITIONS";
        }
        if (ctx instanceof KafkaParser.CmdAlterGroupResetOffsetContext) {
            return "ALTER GROUP RESET OFFSET";
        }
        if (ctx instanceof KafkaParser.CmdConsumeContext) {
            return "CONSUME";
        }
        return "KAFKA";
    }
}
