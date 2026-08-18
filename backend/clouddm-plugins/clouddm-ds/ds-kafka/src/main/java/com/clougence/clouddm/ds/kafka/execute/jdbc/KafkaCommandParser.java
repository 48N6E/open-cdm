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
        if ("CONSUME".equals(first) && tokens.size() >= 2) {
            return parseConsume(tokens);
        }
        throw new SQLException("unsupported Kafka command. supported: SHOW TOPICS, SHOW GROUPS, DESCRIBE CLUSTER, DESCRIBE TOPIC <name>, CONSUME <topic> [FROM BEGINNING|LATEST] [PARTITION n] [LIMIT n]");
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
            command.setType(KafkaCommandType.DESCRIBE_TOPIC);
            command.setTopic(unquote(tokens.get(2)));
            return command;
        }
        throw new SQLException("unsupported DESCRIBE command. use DESCRIBE CLUSTER or DESCRIBE TOPIC <name>.");
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
