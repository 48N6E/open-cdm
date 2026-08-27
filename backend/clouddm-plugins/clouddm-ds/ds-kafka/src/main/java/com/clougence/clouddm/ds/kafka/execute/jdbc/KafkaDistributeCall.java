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
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.ConfigResource;

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
                case DESCRIBE_TOPIC_INFO:
                    receive.responseResult(request, describeTopicInfo(conn, command, request));
                    break;
                case ALTER_TOPIC:
                    receive.responseResult(request, alterTopic(conn, command, request));
                    break;
                case DESCRIBE_GROUP:
                    receive.responseResult(request, describeGroup(conn, command, request));
                    break;
                case DELETE_GROUP:
                    receive.responseResult(request, deleteGroup(conn, command, request));
                    break;
                case ALTER_GROUP_RESET_OFFSET:
                    receive.responseResult(request, resetGroupOffset(conn, command, request));
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

    private static com.clougence.drivers.adapter.AdapterResultCursor describeTopicInfo(KafkaConnection conn, KafkaCommand command, AdapterRequest request) throws Exception {
        TopicDescription description = KafkaAdminCompat.describeTopic(conn.getAdmin(), command.getTopic(), timeoutSec(conn), TimeUnit.SECONDS);
        int partitionCount = description.partitions().size();
        int replicationFactor = description.partitions().isEmpty() ? 0 : description.partitions().get(0).replicas().size();
        String retentionMs = "";
        String retentionBytes = "";
        String cleanupPolicy = "";
        String minIsr = "";
        try {
            ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, command.getTopic());
            Config config = conn.getAdmin().describeConfigs(Collections.singleton(resource)).all().get(timeoutSec(conn), TimeUnit.SECONDS).get(resource);
            retentionMs = configValue(config, "retention.ms");
            retentionBytes = configValue(config, "retention.bytes");
            cleanupPolicy = configValue(config, "cleanup.policy");
            minIsr = configValue(config, "min.insync.replicas");
        } catch (Exception e) {
            // Topic metadata is still useful when config API is unavailable.
        }
        List<Map<String, Object>> rows = new ArrayList<>();
        rows.add(KafkaUtils.row("TOPIC", command.getTopic(), "PARTITION_COUNT", partitionCount, "REPLICATION_FACTOR", replicationFactor, "MIN_ISR", parseIntOrDefault(minIsr, 1),
                "RETENTION_MS", parseLongOrDefault(retentionMs, -1L), "RETENTION_BYTES", parseLongOrDefault(retentionBytes, -1L), "CLEANUP_POLICY", cleanupPolicy));
        return KafkaUtils.rows(request, KafkaUtils.columns(KafkaUtils.TOPIC, KafkaUtils.PARTITION_COUNT, KafkaUtils.REPLICATION_FACTOR, KafkaUtils.MIN_ISR, KafkaUtils.RETENTION_MS,
                KafkaUtils.RETENTION_BYTES, KafkaUtils.CLEANUP_POLICY), rows);
    }

    private static com.clougence.drivers.adapter.AdapterResultCursor alterTopic(KafkaConnection conn, KafkaCommand command, AdapterRequest request) throws Exception {
        int affected = 0;
        if (StringUtils.isNotBlank(command.getConfigKey())) {
            ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, command.getTopic());
            ConfigEntry entry = new ConfigEntry(command.getConfigKey(), command.getConfigValue());
            AlterConfigOp op = new AlterConfigOp(entry, AlterConfigOp.OpType.SET);
            Map<ConfigResource, Collection<AlterConfigOp>> configs = new LinkedHashMap<>();
            configs.put(resource, Collections.singletonList(op));
            conn.getAdmin().incrementalAlterConfigs(configs).all().get(timeoutSec(conn), TimeUnit.SECONDS);
            affected = 1;
        } else if (command.getTotalPartitions() != null) {
            TopicDescription description = KafkaAdminCompat.describeTopic(conn.getAdmin(), command.getTopic(), timeoutSec(conn), TimeUnit.SECONDS);
            int current = description.partitions().size();
            int target = command.getTotalPartitions();
            if (target <= current) {
                throw new SQLException("partition count must be greater than current count: " + current);
            }
            Map<String, NewPartitions> newPartitions = new LinkedHashMap<>();
            newPartitions.put(command.getTopic(), NewPartitions.increaseTo(target));
            conn.getAdmin().createPartitions(newPartitions).all().get(timeoutSec(conn), TimeUnit.SECONDS);
            affected = target - current;
        } else {
            throw new SQLException("unsupported ALTER TOPIC command.");
        }
        List<Map<String, Object>> rows = new ArrayList<>();
        rows.add(KafkaUtils.row("PROPERTY", command.getConfigKey() == null ? "partitions" : command.getConfigKey(), "VALUE",
                command.getConfigValue() == null ? String.valueOf(command.getTotalPartitions()) : command.getConfigValue(), "AFFECTED", affected));
        return KafkaUtils.rows(request, KafkaUtils.columns(KafkaUtils.PROPERTY, KafkaUtils.VALUE, KafkaUtils.AFFECTED), rows);
    }

    private static com.clougence.drivers.adapter.AdapterResultCursor describeGroup(KafkaConnection conn, KafkaCommand command, AdapterRequest request) throws Exception {
        int timeout = timeoutSec(conn);
        Object description = KafkaAdminCompat.describeGroup(conn.getAdmin(), command.getGroupId(), timeout, TimeUnit.SECONDS);
        Map<TopicPartition, OffsetAndMetadata> committed = KafkaAdminCompat.listGroupOffsets(conn.getAdmin(), command.getGroupId(), timeout, TimeUnit.SECONDS);
        List<TopicPartition> partitions = filterPartitions(committed.keySet(), command.getTopic(), command.getPartition());
        Map<TopicPartition, Object> endOffsets = Collections.emptyMap();
        if (!partitions.isEmpty()) {
            endOffsets = KafkaAdminCompat.listOffsets(conn.getAdmin(), KafkaAdminCompat.offsetSpecs(partitions, OffsetSpec.latest()), timeout, TimeUnit.SECONDS);
        }
        List<Map<String, Object>> rows = new ArrayList<>();
        if (partitions.isEmpty()) {
            rows.add(KafkaUtils.row("GROUP_ID", command.getGroupId(), "STATE", KafkaAdminCompat.groupState(description), "MEMBER_COUNT", KafkaAdminCompat.groupMembers(description), "TOPIC", "",
                    "PARTITION", null, "CURRENT_OFFSET", null, "LOG_END_OFFSET", null, "LAG", null));
        } else {
            for (TopicPartition partition : partitions) {
                OffsetAndMetadata metadata = committed.get(partition);
                long current = metadata == null ? -1L : metadata.offset();
                Object endInfo = endOffsets.get(partition);
                long end = endInfo == null ? -1L : KafkaAdminCompat.listOffsetValue(endInfo);
                Long lag = null;
                if (current >= 0 && end >= 0) {
                    lag = Math.max(0L, end - current);
                }
                rows.add(KafkaUtils.row("GROUP_ID", command.getGroupId(), "STATE", KafkaAdminCompat.groupState(description), "MEMBER_COUNT", KafkaAdminCompat.groupMembers(description), "TOPIC",
                        partition.topic(), "PARTITION", partition.partition(), "CURRENT_OFFSET", current, "LOG_END_OFFSET", end, "LAG", lag));
            }
        }
        return KafkaUtils.rows(request,
                KafkaUtils.columns(KafkaUtils.GROUP_ID, KafkaUtils.STATE, KafkaUtils.MEMBER_COUNT, KafkaUtils.TOPIC, KafkaUtils.PARTITION, KafkaUtils.CURRENT_OFFSET, KafkaUtils.LOG_END_OFFSET, KafkaUtils.LAG),
                rows);
    }

    private static com.clougence.drivers.adapter.AdapterResultCursor deleteGroup(KafkaConnection conn, KafkaCommand command, AdapterRequest request) throws Exception {
        KafkaAdminCompat.deleteGroup(conn.getAdmin(), command.getGroupId(), timeoutSec(conn), TimeUnit.SECONDS);
        List<Map<String, Object>> rows = new ArrayList<>();
        rows.add(KafkaUtils.row("PROPERTY", "group", "VALUE", command.getGroupId(), "AFFECTED", 1));
        return KafkaUtils.rows(request, KafkaUtils.columns(KafkaUtils.PROPERTY, KafkaUtils.VALUE, KafkaUtils.AFFECTED), rows);
    }

    private static com.clougence.drivers.adapter.AdapterResultCursor resetGroupOffset(KafkaConnection conn, KafkaCommand command, AdapterRequest request) throws Exception {
        int timeout = timeoutSec(conn);
        Map<TopicPartition, OffsetAndMetadata> committed = KafkaAdminCompat.listGroupOffsets(conn.getAdmin(), command.getGroupId(), timeout, TimeUnit.SECONDS);
        List<TopicPartition> partitions = filterPartitions(committed.keySet(), command.getTopic(), command.getPartition());
        if (partitions.isEmpty() && StringUtils.isNotBlank(command.getTopic())) {
            TopicDescription description = KafkaAdminCompat.describeTopic(conn.getAdmin(), command.getTopic(), timeout, TimeUnit.SECONDS);
            partitions = new ArrayList<>();
            for (TopicPartitionInfo info : description.partitions()) {
                if (command.getPartition() != null && command.getPartition() != info.partition()) {
                    continue;
                }
                partitions.add(new TopicPartition(command.getTopic(), info.partition()));
            }
        }
        if (partitions.isEmpty()) {
            throw new SQLException("no topic partitions found for consumer group reset.");
        }
        Map<TopicPartition, Long> targetOffsets = resolveResetOffsets(conn, command, partitions, timeout);
        Map<TopicPartition, OffsetAndMetadata> updates = new LinkedHashMap<>();
        for (Map.Entry<TopicPartition, Long> entry : targetOffsets.entrySet()) {
            updates.put(entry.getKey(), new OffsetAndMetadata(entry.getValue()));
        }
        KafkaAdminCompat.alterGroupOffsets(conn.getAdmin(), command.getGroupId(), updates, timeout, TimeUnit.SECONDS);
        List<Map<String, Object>> rows = new ArrayList<>();
        for (Map.Entry<TopicPartition, Long> entry : targetOffsets.entrySet()) {
            rows.add(KafkaUtils.row("GROUP_ID", command.getGroupId(), "TOPIC", entry.getKey().topic(), "PARTITION", entry.getKey().partition(), "CURRENT_OFFSET", entry.getValue(), "AFFECTED", 1));
        }
        return KafkaUtils.rows(request, KafkaUtils.columns(KafkaUtils.GROUP_ID, KafkaUtils.TOPIC, KafkaUtils.PARTITION, KafkaUtils.CURRENT_OFFSET, KafkaUtils.AFFECTED), rows);
    }

    private static Map<TopicPartition, Long> resolveResetOffsets(KafkaConnection conn, KafkaCommand command, List<TopicPartition> partitions, int timeout) throws Exception {
        Map<TopicPartition, Long> result = new LinkedHashMap<>();
        if (command.getResetMode() == KafkaResetMode.OFFSET) {
            long offset = command.getResetOffset() == null ? 0L : command.getResetOffset();
            for (TopicPartition partition : partitions) {
                result.put(partition, offset);
            }
            return result;
        }
        OffsetSpec spec;
        if (command.getResetMode() == KafkaResetMode.BEGINNING) {
            spec = OffsetSpec.earliest();
        } else if (command.getResetMode() == KafkaResetMode.LATEST) {
            spec = OffsetSpec.latest();
        } else if (command.getResetMode() == KafkaResetMode.TIMESTAMP) {
            if (command.getResetTimestamp() == null) {
                throw new SQLException("reset timestamp is required.");
            }
            spec = OffsetSpec.forTimestamp(command.getResetTimestamp());
        } else {
            throw new SQLException("unsupported reset mode: " + command.getResetMode());
        }
        Map<TopicPartition, Object> listed = KafkaAdminCompat.listOffsets(conn.getAdmin(), KafkaAdminCompat.offsetSpecs(partitions, spec), timeout, TimeUnit.SECONDS);
        for (TopicPartition partition : partitions) {
            Object info = listed.get(partition);
            if (info == null) {
                throw new SQLException("offset not found for " + partition);
            }
            long offset = KafkaAdminCompat.listOffsetValue(info);
            if (offset < 0 && command.getResetMode() == KafkaResetMode.TIMESTAMP) {
                // No message at/after timestamp: fall back to latest.
                Map<TopicPartition, Object> latest = KafkaAdminCompat.listOffsets(conn.getAdmin(), KafkaAdminCompat.offsetSpecs(Collections.singletonList(partition), OffsetSpec.latest()), timeout,
                        TimeUnit.SECONDS);
                offset = KafkaAdminCompat.listOffsetValue(latest.get(partition));
            }
            result.put(partition, offset);
        }
        return result;
    }

    private static List<TopicPartition> filterPartitions(Collection<TopicPartition> source, String topic, Integer partition) {
        List<TopicPartition> result = new ArrayList<>();
        for (TopicPartition item : source) {
            if (StringUtils.isNotBlank(topic) && !topic.equals(item.topic())) {
                continue;
            }
            if (partition != null && partition != item.partition()) {
                continue;
            }
            result.add(item);
        }
        result.sort((left, right) -> {
            int topicCmp = left.topic().compareTo(right.topic());
            if (topicCmp != 0) {
                return topicCmp;
            }
            return Integer.compare(left.partition(), right.partition());
        });
        return result;
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

    private static String configValue(Config config, String key) {
        if (config == null) {
            return "";
        }
        ConfigEntry entry = config.get(key);
        if (entry == null) {
            return "";
        }
        return entry.value();
    }

    private static int parseIntOrDefault(String value, int defaultValue) {
        if (StringUtils.isBlank(value)) {
            return defaultValue;
        }
        return Integer.parseInt(value);
    }

    private static long parseLongOrDefault(String value, long defaultValue) {
        if (StringUtils.isBlank(value)) {
            return defaultValue;
        }
        return Long.parseLong(value);
    }
}
