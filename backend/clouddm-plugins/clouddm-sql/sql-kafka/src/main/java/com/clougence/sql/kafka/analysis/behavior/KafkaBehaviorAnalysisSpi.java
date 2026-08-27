/*
 * Copyright 2026 杭州开云集致科技有限公司
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package com.clougence.sql.kafka.analysis.behavior;

import java.io.Reader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;

import com.clougence.clouddm.sdk.sql.analysis.behavior.*;
import com.clougence.clouddm.sdk.sql.parser.SplitQueryType;
import com.clougence.schema.umi.struts.UmiTypes;
import com.clougence.sql.kafka.analysis.KafkaAnalysisHelper;
import com.clougence.sql.kafka.parser.KafkaSplitAnalysisSpi;
import com.clougence.sql.kafka.parser.KafkaSplitVisitor;
import com.clougence.sql.kafka.parser.antlr.KafkaLexer;
import com.clougence.sql.kafka.parser.antlr.KafkaParser;
import com.clougence.utils.StringUtils;

public class KafkaBehaviorAnalysisSpi implements BehaviorAnalysisSpi {

    @Override
    public Stream<StatementBehavior> analysisBehaviorStream(Reader queryReader, Map<UmiTypes, Object> levels, int baseLine, int baseColumn) {
        var scripts = new KafkaSplitAnalysisSpi().splitScriptStream(queryReader, List.of(), baseLine, baseColumn);
        return scripts.flatMap(script -> {
            StringReader reader = new StringReader(script.getScript());
            int codeLine = script.getBodyStartCodeLine();
            int codeColumn = script.getBodyStartCodeColumn();
            return analyzeStatement(reader, levels, codeLine, codeColumn).stream();
        }).onClose(scripts::close);
    }

    private List<StatementBehavior> analyzeStatement(Reader queryReader, Map<UmiTypes, Object> levels, int baseLine, int baseColumn) {
        List<StatementBehavior> result = new ArrayList<>();
        try {
            KafkaLexer lexer = new KafkaLexer(CharStreams.fromReader(queryReader));
            KafkaParser parser = new KafkaParser(new CommonTokenStream(lexer));
            KafkaParser.RootInstSetContext root = parser.rootInstSet();
            if (root.commands() == null) {
                return result;
            }
            for (KafkaParser.CmdInstContext cmdInst : root.commands().cmdInst()) {
                KafkaParser.CommandContext command = cmdInst.command();
                SplitQueryType statementType = command.accept(KafkaSplitVisitor.INSTANCE);
                StatementBehavior behavior = new StatementBehavior();
                behavior.setStatementType(statementType);

                String topic = null;
                if (command instanceof KafkaParser.CmdDescribeTopicInfoContext describeTopicInfo) {
                    topic = KafkaAnalysisHelper.topicName(describeTopicInfo.topicName());
                } else if (command instanceof KafkaParser.CmdDescribeTopicContext describeTopic) {
                    topic = KafkaAnalysisHelper.topicName(describeTopic.topicName());
                } else if (command instanceof KafkaParser.CmdDescribeGroupContext describeGroup) {
                    topic = KafkaAnalysisHelper.topicName(describeGroup.topicName());
                } else if (command instanceof KafkaParser.CmdAlterTopicSetContext alterTopicSet) {
                    topic = KafkaAnalysisHelper.topicName(alterTopicSet.topicName());
                } else if (command instanceof KafkaParser.CmdAlterTopicAddPartitionsContext alterTopicAddPartitions) {
                    topic = KafkaAnalysisHelper.topicName(alterTopicAddPartitions.topicName());
                } else if (command instanceof KafkaParser.CmdAlterGroupResetOffsetContext alterGroup) {
                    for (KafkaParser.ResetScopeContext scope : alterGroup.resetScope()) {
                        if (scope.topicName() != null) {
                            topic = KafkaAnalysisHelper.topicName(scope.topicName());
                            break;
                        }
                    }
                } else if (command instanceof KafkaParser.CmdConsumeContext consume) {
                    topic = KafkaAnalysisHelper.topicName(consume.topicName());
                }

                BehaviorObject object = new BehaviorObject();
                if (StringUtils.isNotBlank(topic)) {
                    object.setObjectType(TargetType.Table);
                    object.setObjectPath(resourcePath(levels, topic));
                    object.setObjectName(new ObjectName(null, level(levels, UmiTypes.Schema), topic));
                } else {
                    object.setObjectType(TargetType.Schema);
                    object.setObjectPath(resourcePath(levels, null));
                    object.setObjectName(new ObjectName(null, level(levels, UmiTypes.Schema), null));
                }
                object.setStartLine(baseLine);
                object.setStartColumn(baseColumn);
                object.setEndLine(baseLine);
                object.setEndColumn(baseColumn + command.getText().length());

                BehaviorRelation relation = new BehaviorRelation();
                relation.setSubject(object);
                relation.setAction(action(statementType));
                behavior.getRelations().add(relation);
                result.add(behavior);
            }
        } catch (Exception ignored) {
            return result;
        }
        return result;
    }

    private String resourcePath(Map<UmiTypes, Object> levels, String topic) {
        List<String> nodes = new ArrayList<>();
        addPath(nodes, levels == null ? null : levels.get(UmiTypes.Instance));
        addPath(nodes, level(levels, UmiTypes.Schema));
        addPath(nodes, topic);
        return "/" + String.join("/", nodes) + "/";
    }

    private void addPath(List<String> nodes, Object value) {
        if (value == null) {
            return;
        }
        String path = StringUtils.toString(value);
        int start = 0;
        for (int i = 0; i <= path.length(); i++) {
            if (i == path.length() || path.charAt(i) == '/') {
                String node = path.substring(start, i);
                if (StringUtils.isNotBlank(node)) {
                    nodes.add(node);
                }
                start = i + 1;
            }
        }
    }

    private String level(Map<UmiTypes, Object> levels, UmiTypes type) {
        return levels == null || levels.get(type) == null ? null : StringUtils.toString(levels.get(type));
    }

    private BehaviorAction action(SplitQueryType type) {
        return switch (type) {
            case SELECT, METADATA, PERFORMANCE, LOG_READ -> BehaviorAction.READ;
            case INSERT -> BehaviorAction.INSERT;
            case UPDATE, ALTER_TABLE, MERGE -> BehaviorAction.UPDATE;
            case DELETE -> BehaviorAction.DELETE;
            default -> BehaviorAction.UNKNOWN;
        };
    }
}
