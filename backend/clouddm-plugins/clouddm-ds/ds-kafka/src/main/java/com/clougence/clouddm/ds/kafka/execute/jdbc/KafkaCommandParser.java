/*
 * Copyright 2026 杭州开云集致科技有限公司
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.clougence.clouddm.ds.kafka.execute.jdbc;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import com.clougence.utils.StringUtils;

public class KafkaCommandParser {

    public static KafkaCommand parse(String body) throws SQLException {
        String sql = StringUtils.trimToEmpty(body);
        if (sql.endsWith(";")) {
            sql = sql.substring(0, sql.length() - 1).trim();
        }
        if (StringUtils.isBlank(sql)) {
            throw new SQLException("query command is empty.");
        }
        List<String> tokens = tokenize(sql);
        if (tokens.isEmpty()) {
            throw new SQLException("query command is empty.");
        }
        String first = tokens.get(0).toUpperCase(Locale.ROOT);
        if ("SHOW".equals(first) && tokens.size() >= 2) {
            return parseShow(tokens);
        }
        if ("DESCRIBE".equals(first) && tokens.size() >= 2) {
            return parseDescribe(tokens);
        }
        if ("DELETE".equals(first) && tokens.size() >= 3) {
            return parseDelete(tokens);
        }
        if ("CONSUME".equals(first) && tokens.size() >= 2) {
            return parseConsume(tokens);
        }
        if ("ALTER".equals(first) && tokens.size() >= 3) {
            return parseAlter(tokens);
        }
        throw new SQLException(
                "unsupported Kafka command. supported: SHOW TOPICS, SHOW GROUPS, DESCRIBE CLUSTER, DESCRIBE TOPIC <name>, DESCRIBE TOPIC <name> INFO, DESCRIBE GROUP <id>, DELETE GROUP <id>, ALTER TOPIC ..., ALTER GROUP <id> RESET OFFSET TO <value|BEGINNING|LATEST|TIMESTAMP <ms>> [TOPIC <t>] [PARTITION <p>], CONSUME ...");
    }

    private static KafkaCommand parseShow(List<String> tokens) throws SQLException {
        String second = tokens.get(1).toUpperCase(Locale.ROOT);
        KafkaCommand command = new KafkaCommand();
        if ("TOPICS".equals(second)) {
            command.setType(KafkaCommandType.SHOW_TOPICS);
            return command;
        }
        if ("GROUPS".equals(second)) {
            command.setType(KafkaCommandType.SHOW_GROUPS);
            return command;
        }
        throw new SQLException("unsupported SHOW command. use SHOW TOPICS or SHOW GROUPS.");
    }

    private static KafkaCommand parseDescribe(List<String> tokens) throws SQLException {
        String second = tokens.get(1).toUpperCase(Locale.ROOT);
        KafkaCommand command = new KafkaCommand();
        if ("CLUSTER".equals(second)) {
            command.setType(KafkaCommandType.DESCRIBE_CLUSTER);
            return command;
        }
        if ("TOPIC".equals(second) && tokens.size() >= 3) {
            command.setTopic(unquote(tokens.get(2)));
            if (tokens.size() >= 4 && "INFO".equalsIgnoreCase(tokens.get(3))) {
                command.setType(KafkaCommandType.DESCRIBE_TOPIC_INFO);
                return command;
            }
            command.setType(KafkaCommandType.DESCRIBE_TOPIC);
            return command;
        }
        if ("GROUP".equals(second) && tokens.size() >= 3) {
            command.setType(KafkaCommandType.DESCRIBE_GROUP);
            command.setGroupId(unquote(tokens.get(2)));
            if (tokens.size() >= 5 && "TOPIC".equalsIgnoreCase(tokens.get(3))) {
                command.setTopic(unquote(tokens.get(4)));
            }
            return command;
        }
        throw new SQLException("unsupported DESCRIBE command. use DESCRIBE CLUSTER, DESCRIBE TOPIC <name>, DESCRIBE TOPIC <name> INFO or DESCRIBE GROUP <id>.");
    }

    private static KafkaCommand parseDelete(List<String> tokens) throws SQLException {
        if (!"GROUP".equalsIgnoreCase(tokens.get(1)) || tokens.size() < 3) {
            throw new SQLException("unsupported DELETE command. use DELETE GROUP <id>.");
        }
        KafkaCommand command = new KafkaCommand();
        command.setType(KafkaCommandType.DELETE_GROUP);
        command.setGroupId(unquote(tokens.get(2)));
        return command;
    }

    private static KafkaCommand parseConsume(List<String> tokens) throws SQLException {
        KafkaCommand command = new KafkaCommand();
        command.setType(KafkaCommandType.CONSUME);
        command.setTopic(unquote(tokens.get(1)));
        int i = 2;
        while (i < tokens.size()) {
            String token = tokens.get(i).toUpperCase(Locale.ROOT);
            if ("FROM".equals(token) && i + 1 < tokens.size()) {
                String pos = tokens.get(i + 1).toUpperCase(Locale.ROOT);
                command.setFromBeginning(!"LATEST".equals(pos));
                i += 2;
                continue;
            }
            if ("PARTITION".equals(token) && i + 1 < tokens.size()) {
                command.setPartition(Integer.parseInt(tokens.get(i + 1)));
                i += 2;
                continue;
            }
            if ("LIMIT".equals(token) && i + 1 < tokens.size()) {
                command.setLimit(Integer.parseInt(tokens.get(i + 1)));
                i += 2;
                continue;
            }
            throw new SQLException("unsupported CONSUME option: " + tokens.get(i));
        }
        return command;
    }

    private static KafkaCommand parseAlter(List<String> tokens) throws SQLException {
        String target = tokens.get(1).toUpperCase(Locale.ROOT);
        if ("TOPIC".equals(target)) {
            return parseAlterTopic(tokens);
        }
        if ("GROUP".equals(target)) {
            return parseAlterGroup(tokens);
        }
        throw new SQLException("unsupported ALTER command. use ALTER TOPIC ... or ALTER GROUP ... RESET OFFSET ...");
    }

    private static KafkaCommand parseAlterTopic(List<String> tokens) throws SQLException {
        if (tokens.size() < 4) {
            throw new SQLException("unsupported ALTER command. use ALTER TOPIC <name> SET <key> = <value> or ALTER TOPIC <name> ADD PARTITIONS <count>.");
        }
        KafkaCommand command = new KafkaCommand();
        command.setType(KafkaCommandType.ALTER_TOPIC);
        command.setTopic(unquote(tokens.get(2)));
        String action = tokens.get(3).toUpperCase(Locale.ROOT);
        if ("SET".equals(action)) {
            if (tokens.size() < 7 || !"=".equals(tokens.get(5))) {
                throw new SQLException("unsupported ALTER command. use ALTER TOPIC <name> SET <key> = <value>.");
            }
            command.setConfigKey(tokens.get(4));
            command.setConfigValue(unquote(tokens.get(6)));
            return command;
        }
        if ("ADD".equals(action) && tokens.size() >= 6 && "PARTITIONS".equalsIgnoreCase(tokens.get(4))) {
            command.setTotalPartitions(Integer.parseInt(tokens.get(5)));
            return command;
        }
        throw new SQLException("unsupported ALTER command. use ALTER TOPIC <name> SET <key> = <value> or ALTER TOPIC <name> ADD PARTITIONS <count>.");
    }

    private static KafkaCommand parseAlterGroup(List<String> tokens) throws SQLException {
        // ALTER GROUP <id> RESET OFFSET TO <n|BEGINNING|LATEST|TIMESTAMP <ms>> [TOPIC <t>] [PARTITION <p>]
        if (tokens.size() < 6 || !"RESET".equalsIgnoreCase(tokens.get(3)) || !"OFFSET".equalsIgnoreCase(tokens.get(4)) || !"TO".equalsIgnoreCase(tokens.get(5))) {
            throw new SQLException("unsupported ALTER GROUP command. use ALTER GROUP <id> RESET OFFSET TO <value|BEGINNING|LATEST|TIMESTAMP <ms>> [TOPIC <t>] [PARTITION <p>].");
        }
        KafkaCommand command = new KafkaCommand();
        command.setType(KafkaCommandType.ALTER_GROUP_RESET_OFFSET);
        command.setGroupId(unquote(tokens.get(2)));
        int i = 6;
        if (i >= tokens.size()) {
            throw new SQLException("missing RESET OFFSET target.");
        }
        String target = tokens.get(i).toUpperCase(Locale.ROOT);
        if ("BEGINNING".equals(target)) {
            command.setResetMode(KafkaResetMode.BEGINNING);
            i++;
        } else if ("LATEST".equals(target)) {
            command.setResetMode(KafkaResetMode.LATEST);
            i++;
        } else if ("TIMESTAMP".equals(target)) {
            if (i + 1 >= tokens.size()) {
                throw new SQLException("missing TIMESTAMP value.");
            }
            command.setResetMode(KafkaResetMode.TIMESTAMP);
            command.setResetTimestamp(Long.parseLong(tokens.get(i + 1)));
            i += 2;
        } else {
            command.setResetMode(KafkaResetMode.OFFSET);
            command.setResetOffset(Long.parseLong(tokens.get(i)));
            i++;
        }
        while (i < tokens.size()) {
            String token = tokens.get(i).toUpperCase(Locale.ROOT);
            if ("TOPIC".equals(token) && i + 1 < tokens.size()) {
                command.setTopic(unquote(tokens.get(i + 1)));
                i += 2;
                continue;
            }
            if ("PARTITION".equals(token) && i + 1 < tokens.size()) {
                command.setPartition(Integer.parseInt(tokens.get(i + 1)));
                i += 2;
                continue;
            }
            throw new SQLException("unsupported ALTER GROUP option: " + tokens.get(i));
        }
        if (command.getPartition() != null && StringUtils.isBlank(command.getTopic())) {
            throw new SQLException("PARTITION requires TOPIC.");
        }
        return command;
    }

    private static List<String> tokenize(String sql) {
        List<String> tokens = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean inQuote = false;
        char quote = 0;
        for (int i = 0; i < sql.length(); i++) {
            char c = sql.charAt(i);
            if (inQuote) {
                if (c == quote) {
                    inQuote = false;
                } else {
                    current.append(c);
                }
                continue;
            }
            if (c == '\'' || c == '"') {
                inQuote = true;
                quote = c;
                continue;
            }
            if (Character.isWhitespace(c)) {
                if (current.length() > 0) {
                    tokens.add(current.toString());
                    current.setLength(0);
                }
                continue;
            }
            current.append(c);
        }
        if (current.length() > 0) {
            tokens.add(current.toString());
        }
        return tokens;
    }

    private static String unquote(String token) {
        if (token.length() >= 2) {
            char first = token.charAt(0);
            char last = token.charAt(token.length() - 1);
            if ((first == '"' && last == '"') || (first == '\'' && last == '\'')) {
                return token.substring(1, token.length() - 1);
            }
        }
        return token;
    }
}
