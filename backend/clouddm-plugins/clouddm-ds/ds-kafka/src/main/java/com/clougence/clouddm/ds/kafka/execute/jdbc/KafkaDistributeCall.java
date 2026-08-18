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
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;

import com.clougence.drivers.adapter.AdapterReceive;
import com.clougence.drivers.adapter.AdapterRequest;
import com.clougence.utils.StringUtils;
import com.clougence.utils.future.CgFuture;

class KafkaDistributeCall {

    static void exec(CgFuture<Object> sync, KafkaConnection conn, KafkaCommand command, AdapterRequest request, AdapterReceive receive) throws SQLException {
        try {
            switch (command.getType()) {
                case SHOW_TOPICS:
                    receive.responseResult(request, showTopics(conn, request));
                    break;
                case SHOW_GROUPS:
                    receive.responseResult(request, showGroups(conn, request));
                    break;
                case DESCRIBE_CLUSTER:
                    receive.responseResult(request, describeCluster(conn, request));
                    break;
                case DESCRIBE_TOPIC:
                    receive.responseResult(request, describeTopic(conn, command, request));
                    break;
                case CONSUME:
                    receive.responseResult(request, consume(conn, command, request));
                    break;
                default:
                    throw new SQLException("unsupported Kafka command: " + command.getType());
            }
            KafkaUtils.completed(sync);
        } catch (Exception e) {
            KafkaUtils.failed(sync, e);
            if (e instanceof SQLException) {
                throw (SQLException) e;
            }
            throw new SQLException(e.getMessage(), e);
        }
    }

    private static com.clougence.drivers.adapter.AdapterResultCursor showTopics(KafkaConnection conn, AdapterRequest request) throws Exception {
        Set<String> names = new TreeSet<>(conn.getAdmin().listTopics().names().get(timeoutSec(conn), TimeUnit.SECONDS));
        List<Map<String, Object>> rows = new ArrayList<>();
        for (String name : names) {
            rows.add(KafkaUtils.row("TOPIC", name));
        }
        return KafkaUtils.rows(request, KafkaUtils.columns(KafkaUtils.TOPIC), rows);
    }

    private static com.clougence.drivers.adapter.AdapterResultCursor showGroups(KafkaConnection conn, AdapterRequest request) throws Exception {
        Collection<?> groups = KafkaAdminCompat.listGroups(conn.getAdmin(), timeoutSec(conn), TimeUnit.SECONDS);
        List<Map<String, Object>> rows = new ArrayList<>();
        for (Object group : groups) {
            rows.add(KafkaUtils.row("GROUP_ID", KafkaAdminCompat.listingId(group), "STATE", KafkaAdminCompat.listingState(group)));
        }
        return KafkaUtils.rows(request, KafkaUtils.columns(KafkaUtils.GROUP_ID, KafkaUtils.STATE), rows);
    }

    private static com.clougence.drivers.adapter.AdapterResultCursor describeCluster(KafkaConnection conn, AdapterRequest request) throws Exception {
        DescribeClusterResult cluster = conn.getAdmin().describeCluster();
        int timeout = timeoutSec(conn);
        String clusterId = cluster.clusterId().get(timeout, TimeUnit.SECONDS);
        Node controller = cluster.controller().get(timeout, TimeUnit.SECONDS);
        Collection<Node> nodes = cluster.nodes().get(timeout, TimeUnit.SECONDS);
        List<Map<String, Object>> rows = new ArrayList<>();
        rows.add(KafkaUtils.row("NAME", "clusterId", "VALUE", clusterId));
        rows.add(KafkaUtils.row("NAME", "controller", "VALUE", controller == null ? "" : controller.id() + "@" + controller.host() + ":" + controller.port()));
        rows.add(KafkaUtils.row("NAME", "nodes", "VALUE", String.valueOf(nodes.size())));
        return KafkaUtils.rows(request, KafkaUtils.columns(KafkaUtils.NAME, KafkaUtils.VALUE), rows);
    }

    private static com.clougence.drivers.adapter.AdapterResultCursor describeTopic(KafkaConnection conn, KafkaCommand command, AdapterRequest request) throws Exception {
        TopicDescription description = KafkaAdminCompat.describeTopic(conn.getAdmin(), command.getTopic(), timeoutSec(conn), TimeUnit.SECONDS);
        List<Map<String, Object>> rows = new ArrayList<>();
        for (TopicPartitionInfo info : description.partitions()) {
            rows.add(KafkaUtils.row("PARTITION", info.partition(), "LEADER", info.leader() == null ? -1 : info.leader().id(), "REPLICAS", ids(info.replicas()), "ISR", ids(info.isr())));
        }
        return KafkaUtils.rows(request, KafkaUtils.columns(KafkaUtils.PARTITION, KafkaUtils.LEADER, KafkaUtils.REPLICAS, KafkaUtils.ISR), rows);
    }

    private static com.clougence.drivers.adapter.AdapterResultCursor consume(KafkaConnection conn, KafkaCommand command, AdapterRequest request) throws Exception {
        int limit = command.getLimit();
        if (request.getMaxRows() > 0 && request.getMaxRows() < limit) {
            limit = (int) request.getMaxRows();
        }
        TopicDescription description = KafkaAdminCompat.describeTopic(conn.getAdmin(), command.getTopic(), timeoutSec(conn), TimeUnit.SECONDS);
        List<TopicPartition> partitions = new ArrayList<>();
        for (TopicPartitionInfo info : description.partitions()) {
            if (command.getPartition() != null && command.getPartition() != info.partition()) {
                continue;
            }
            partitions.add(new TopicPartition(command.getTopic(), info.partition()));
        }
        if (partitions.isEmpty()) {
            throw new SQLException("topic partition not found: " + command.getTopic());
        }
        List<Map<String, Object>> rows = new ArrayList<>();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(KafkaClientProps.consumerProps(conn.getDsConfig(), conn.getBootstrap()))) {
            consumer.assign(partitions);
            if (command.isFromBeginning()) {
                consumer.seekToBeginning(partitions);
            } else {
                consumer.seekToEnd(partitions);
            }
            int remainPolls = 8;
            while (rows.size() < limit && remainPolls-- > 0) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                if (records.isEmpty()) {
                    continue;
                }
                for (ConsumerRecord<String, String> record : records) {
                    rows.add(KafkaUtils.row("PARTITION", record.partition(), "OFFSET", record.offset(), "TIMESTAMP", record.timestamp(), "KEY", record.key(), "VALUE", record.value()));
                    if (rows.size() >= limit) {
                        break;
                    }
                }
            }
        }
        return KafkaUtils.rows(request, KafkaUtils.columns(KafkaUtils.PARTITION, KafkaUtils.OFFSET, KafkaUtils.TIMESTAMP, KafkaUtils.KEY, KafkaUtils.MSG_VALUE), rows);
    }

    private static String ids(List<Node> nodes) {
        return nodes.stream().map(node -> String.valueOf(node.id())).collect(Collectors.joining(","));
    }

    private static int timeoutSec(KafkaConnection conn) {
        String soTimeout = conn.getDsConfig().get(KafkaKeys.SO_TIMEOUT);
        if (StringUtils.isBlank(soTimeout)) {
            return 10;
        }
        int ms = Integer.parseInt(soTimeout);
        int sec = ms / 1000;
        if (sec < 1) {
            return 1;
        }
        return sec;
    }
}
