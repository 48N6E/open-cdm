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

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.NewPartitionReassignment;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import com.clougence.utils.ExceptionUtils;
import com.clougence.utils.StringUtils;

/**
 * Runs on the Kafka driver ClassLoader (has kafka-clients).
 */
public final class KafkaOpsAdmin {

    private static final long ADMIN_TIMEOUT_MS = 30000L;

    private KafkaOpsAdmin(){
    }

    public static void createTopic(Connection connection, String topicName, int partitions, short replicationFactor, Map<String, String> configs) throws Exception {
        AdminClient admin = unwrapAdmin(connection);
        NewTopic topic = new NewTopic(topicName, partitions, replicationFactor);
        if (configs != null && !configs.isEmpty()) {
            topic.configs(configs);
        }
        admin.createTopics(Collections.singleton(topic)).all().get(ADMIN_TIMEOUT_MS, TimeUnit.MILLISECONDS);
    }

    public static void deleteTopic(Connection connection, String topicName) throws Exception {
        unwrapAdmin(connection).deleteTopics(Collections.singleton(topicName)).all().get(ADMIN_TIMEOUT_MS, TimeUnit.MILLISECONDS);
    }

    public static Map<String, String> describeTopicConfigs(Connection connection, String topicName) throws Exception {
        AdminClient admin = unwrapAdmin(connection);
        ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topicName);
        Map<ConfigResource, Config> result = admin.describeConfigs(Collections.singleton(resource)).all().get(ADMIN_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        Config config = result.get(resource);
        Map<String, String> map = new LinkedHashMap<>();
        if (config == null) {
            return map;
        }
        for (ConfigEntry entry : config.entries()) {
            if (entry == null || entry.isDefault() || entry.isReadOnly() || entry.isSensitive()) {
                continue;
            }
            map.put(entry.name(), entry.value());
        }
        return map;
    }

    public static void alterTopicConfigs(Connection connection, String topicName, Map<String, String> configs) throws Exception {
        if (configs == null || configs.isEmpty()) {
            return;
        }
        ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topicName);
        Collection<AlterConfigOp> ops = new ArrayList<>();
        for (Map.Entry<String, String> entry : configs.entrySet()) {
            if (StringUtils.isBlank(entry.getKey())) {
                continue;
            }
            ops.add(new AlterConfigOp(new ConfigEntry(entry.getKey(), entry.getValue()), AlterConfigOp.OpType.SET));
        }
        unwrapAdmin(connection).incrementalAlterConfigs(Collections.singletonMap(resource, ops)).all().get(ADMIN_TIMEOUT_MS, TimeUnit.MILLISECONDS);
    }

    public static void alterTopicLayout(Connection connection, String topicName, int partitions, short replicationFactor) throws Exception {
        if (partitions < 1) {
            throw new IllegalArgumentException(KafkaKeys.ERR_INVALID_LAYOUT);
        }
        if (replicationFactor < 1) {
            throw new IllegalArgumentException(KafkaKeys.ERR_INVALID_LAYOUT);
        }
        AdminClient admin = unwrapAdmin(connection);
        Map<String, TopicDescription> descMap = KafkaAdminCompat.awaitTopicDescriptions(admin.describeTopics(Collections.singletonList(topicName)), ADMIN_TIMEOUT_MS);
        TopicDescription description = descMap.get(topicName);
        if (description == null) {
            throw new IllegalArgumentException(KafkaKeys.ERR_TOPIC_NOT_FOUND);
        }
        List<TopicPartitionInfo> currentParts = description.partitions();
        int currentPartitionCount = currentParts == null ? 0 : currentParts.size();
        if (partitions < currentPartitionCount) {
            throw new IllegalArgumentException(KafkaKeys.ERR_PARTITIONS_INCREASE_ONLY);
        }

        int currentRf = 0;
        if (currentParts != null && !currentParts.isEmpty() && currentParts.get(0).replicas() != null) {
            currentRf = currentParts.get(0).replicas().size();
        }

        if (partitions > currentPartitionCount) {
            admin.createPartitions(Collections.singletonMap(topicName, NewPartitions.increaseTo(partitions))).all().get(ADMIN_TIMEOUT_MS, TimeUnit.MILLISECONDS);
            descMap = KafkaAdminCompat.awaitTopicDescriptions(admin.describeTopics(Collections.singletonList(topicName)), ADMIN_TIMEOUT_MS);
            description = descMap.get(topicName);
            currentParts = description == null ? null : description.partitions();
        }

        if (replicationFactor == currentRf) {
            return;
        }

        Collection<Node> nodes = admin.describeCluster().nodes().get(ADMIN_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        List<Integer> brokerIds = new ArrayList<>();
        if (nodes != null) {
            for (Node node : nodes) {
                if (node != null) {
                    brokerIds.add(node.id());
                }
            }
        }
        Collections.sort(brokerIds);
        if (brokerIds.size() < replicationFactor) {
            throw new IllegalArgumentException(KafkaKeys.ERR_REPLICATION_EXCEEDS_BROKERS);
        }
        if (currentParts == null || currentParts.isEmpty()) {
            throw new IllegalArgumentException(KafkaKeys.ERR_TOPIC_NO_PARTITIONS);
        }

        Map<TopicPartition, Optional<NewPartitionReassignment>> reassignments = new HashMap<>();
        for (TopicPartitionInfo part : currentParts) {
            List<Integer> replicas = new ArrayList<>();
            for (int i = 0; i < replicationFactor; i++) {
                int brokerId = brokerIds.get((part.partition() + i) % brokerIds.size());
                if (!replicas.contains(brokerId)) {
                    replicas.add(brokerId);
                }
            }
            while (replicas.size() < replicationFactor) {
                for (Integer brokerId : brokerIds) {
                    if (!replicas.contains(brokerId)) {
                        replicas.add(brokerId);
                    }
                    if (replicas.size() >= replicationFactor) {
                        break;
                    }
                }
            }
            reassignments.put(new TopicPartition(topicName, part.partition()), Optional.of(new NewPartitionReassignment(replicas)));
        }
        admin.alterPartitionReassignments(reassignments).all().get(ADMIN_TIMEOUT_MS, TimeUnit.MILLISECONDS);
    }

    public static void deleteConsumerGroup(Connection connection, String groupId) throws Exception {
        unwrapAdmin(connection).deleteConsumerGroups(Collections.singleton(groupId)).all().get(ADMIN_TIMEOUT_MS, TimeUnit.MILLISECONDS);
    }

    public static void resetConsumerGroupOffsets(Connection connection, String groupId, String mode, List<String> topics, Long timestampMs, Long offset) throws Exception {
        AdminClient admin = unwrapAdmin(connection);
        Set<TopicPartition> partitions = resolvePartitions(admin, groupId, topics);
        if (partitions.isEmpty()) {
            throw new IllegalArgumentException(KafkaKeys.ERR_RESET_NO_PARTITIONS);
        }
        String resetMode = StringUtils.defaultIfBlank(mode, "EARLIEST").toUpperCase();
        Map<TopicPartition, OffsetAndMetadata> target = new HashMap<>();
        if ("OFFSET".equals(resetMode)) {
            if (offset == null || offset < 0) {
                throw new IllegalArgumentException(KafkaKeys.ERR_OFFSET_REQUIRED);
            }
            for (TopicPartition tp : partitions) {
                target.put(tp, new OffsetAndMetadata(offset));
            }
        } else {
            Map<TopicPartition, OffsetSpec> specs = new HashMap<>();
            OffsetSpec spec;
            if ("LATEST".equals(resetMode)) {
                spec = OffsetSpec.latest();
            } else if ("TIMESTAMP".equals(resetMode)) {
                if (timestampMs == null || timestampMs < 0) {
                    throw new IllegalArgumentException(KafkaKeys.ERR_TIMESTAMP_REQUIRED);
                }
                spec = OffsetSpec.forTimestamp(timestampMs);
            } else {
                spec = OffsetSpec.earliest();
            }
            for (TopicPartition tp : partitions) {
                specs.put(tp, spec);
            }
            Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> ends = admin.listOffsets(specs).all().get(ADMIN_TIMEOUT_MS, TimeUnit.MILLISECONDS);
            Set<TopicPartition> needLatest = new HashSet<>();
            for (TopicPartition tp : partitions) {
                ListOffsetsResult.ListOffsetsResultInfo info = ends.get(tp);
                long value = info == null ? -1L : info.offset();
                if (value < 0) {
                    needLatest.add(tp);
                } else {
                    target.put(tp, new OffsetAndMetadata(value));
                }
            }
            if (!needLatest.isEmpty()) {
                Map<TopicPartition, OffsetSpec> latestSpecs = new HashMap<>();
                for (TopicPartition tp : needLatest) {
                    latestSpecs.put(tp, OffsetSpec.latest());
                }
                Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> latestEnds = admin.listOffsets(latestSpecs).all().get(ADMIN_TIMEOUT_MS, TimeUnit.MILLISECONDS);
                for (TopicPartition tp : needLatest) {
                    ListOffsetsResult.ListOffsetsResultInfo info = latestEnds.get(tp);
                    long value = info == null ? 0L : info.offset();
                    if (value < 0) {
                        value = 0L;
                    }
                    target.put(tp, new OffsetAndMetadata(value));
                }
            }
        }
        try {
            admin.alterConsumerGroupOffsets(groupId, target).all().get(ADMIN_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            String root = ExceptionUtils.getRootCauseMessage(e);
            if (root != null && root.contains("GroupNotEmpty")) {
                throw new IllegalStateException(KafkaKeys.ERR_GROUP_NOT_EMPTY, e);
            }
            throw e;
        }
    }

    private static Set<TopicPartition> resolvePartitions(AdminClient admin, String groupId, List<String> topics) throws Exception {
        Set<String> topicFilter = topics == null || topics.isEmpty() ? null : new HashSet<>(topics);
        Map<TopicPartition, OffsetAndMetadata> committed = admin.listConsumerGroupOffsets(groupId).partitionsToOffsetAndMetadata().get(ADMIN_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        Set<TopicPartition> partitions = new HashSet<>();
        if (committed != null) {
            for (TopicPartition tp : committed.keySet()) {
                if (topicFilter == null || topicFilter.contains(tp.topic())) {
                    partitions.add(tp);
                }
            }
        }
        if (!partitions.isEmpty()) {
            return partitions;
        }
        if (topicFilter == null || topicFilter.isEmpty()) {
            return partitions;
        }
        Map<String, TopicDescription> descMap = KafkaAdminCompat.awaitTopicDescriptions(admin.describeTopics(topicFilter), ADMIN_TIMEOUT_MS);
        for (TopicDescription description : descMap.values()) {
            List<TopicPartitionInfo> parts = description.partitions();
            if (parts == null) {
                continue;
            }
            for (TopicPartitionInfo part : parts) {
                partitions.add(new TopicPartition(description.name(), part.partition()));
            }
        }
        return partitions;
    }

    public static List<Map<String, Object>> consumeMessages(Connection connection, String topicName, Integer partition, String mode, int maxMessages,
                                                            long timeoutMs, String groupId) throws Exception {
        if (StringUtils.isBlank(topicName)) {
            throw new IllegalArgumentException(KafkaKeys.ERR_CONSUME_TOPIC_REQUIRED);
        }
        int limit = maxMessages;
        if (limit < 1) {
            limit = 50;
        }
        if (limit > 500) {
            limit = 500;
        }
        long waitMs = timeoutMs;
        if (waitMs < 1000L) {
            waitMs = 5000L;
        }
        if (waitMs > 60000L) {
            waitMs = 60000L;
        }
        String seekMode = StringUtils.defaultIfBlank(mode, "RECENT").trim().toUpperCase();
        String consumerGroupId = StringUtils.trimToNull(groupId);
        if ("GROUP".equals(seekMode) && consumerGroupId == null) {
            throw new IllegalArgumentException(KafkaKeys.ERR_CONSUME_GROUP_REQUIRED);
        }

        Properties consumerProps = new Properties();
        consumerProps.putAll(clientProperties(connection));
        consumerProps.remove(ConsumerConfig.GROUP_ID_CONFIG);
        consumerProps.remove(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG);
        if (consumerGroupId != null) {
            consumerProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);
        }
        consumerProps.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        consumerProps.setProperty(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, "false");
        consumerProps.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "none");
        consumerProps.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, "CloudDM-consume-" + UUID.randomUUID());
        // Pass Class objects so Kafka does not Class.forName via the wrong ClassLoader.
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);

        List<Map<String, Object>> rows = new ArrayList<>();
        Map<TopicPartition, Long> commitOffsets = new HashMap<>();
        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(consumerProps)) {
            Duration metadataTimeout = Duration.ofMillis(Math.min(waitMs, ADMIN_TIMEOUT_MS));
            List<TopicPartition> partitions = resolveConsumePartitions(consumer, topicName, partition, metadataTimeout);
            consumer.assign(partitions);
            seekConsume(consumer, partitions, seekMode, limit, metadataTimeout);

            long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(waitMs);
            while (rows.size() < limit) {
                long remaining = TimeUnit.NANOSECONDS.toMillis(deadline - System.nanoTime());
                if (remaining <= 0) {
                    break;
                }
                ConsumerRecords<byte[], byte[]> batch = consumer.poll(Duration.ofMillis(Math.min(remaining, 1000L)));
                if (batch.isEmpty()) {
                    continue;
                }
                for (ConsumerRecord<byte[], byte[]> record : batch) {
                    rows.add(toMessageRow(record));
                    TopicPartition tp = new TopicPartition(record.topic(), record.partition());
                    commitOffsets.put(tp, record.offset() + 1);
                    if (rows.size() >= limit) {
                        break;
                    }
                }
            }
            if (consumerGroupId != null && !commitOffsets.isEmpty()) {
                Map<TopicPartition, OffsetAndMetadata> toCommit = new HashMap<>();
                for (Map.Entry<TopicPartition, Long> entry : commitOffsets.entrySet()) {
                    toCommit.put(entry.getKey(), new OffsetAndMetadata(entry.getValue()));
                }
                consumer.commitSync(toCommit);
            }
        }
        return rows;
    }

    public static Map<String, Object> produceMessage(Connection connection, String topicName, Integer partition, String key, String value, int count,
                                                      Map<String, String> headers) throws Exception {
        if (StringUtils.isBlank(topicName)) {
            throw new IllegalArgumentException(KafkaKeys.ERR_PRODUCE_TOPIC_REQUIRED);
        }
        if (value == null) {
            throw new IllegalArgumentException(KafkaKeys.ERR_PRODUCE_VALUE_REQUIRED);
        }
        int sendCount = count;
        if (sendCount < 1) {
            sendCount = 1;
        }
        if (sendCount > 500) {
            sendCount = 500;
        }

        Properties producerProps = new Properties();
        producerProps.putAll(clientProperties(connection));
        producerProps.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        producerProps.setProperty(ProducerConfig.CLIENT_ID_CONFIG, "CloudDM-produce-" + UUID.randomUUID());
        // Pass Class objects so Kafka does not Class.forName via the wrong ClassLoader.
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);

        byte[] keyBytes = null;
        if (key != null) {
            keyBytes = key.getBytes(StandardCharsets.UTF_8);
        }
        byte[] valueBytes = value.getBytes(StandardCharsets.UTF_8);

        try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(producerProps)) {
            RecordMetadata last = null;
            Long firstOffset = null;
            for (int i = 0; i < sendCount; i++) {
                ProducerRecord<byte[], byte[]> record;
                if (partition == null) {
                    record = new ProducerRecord<>(topicName, keyBytes, valueBytes);
                } else {
                    record = new ProducerRecord<>(topicName, partition, keyBytes, valueBytes);
                }
                applyHeaders(record, headers);
                last = producer.send(record).get(ADMIN_TIMEOUT_MS, TimeUnit.MILLISECONDS);
                if (firstOffset == null) {
                    firstOffset = last.offset();
                }
            }
            Map<String, Object> result = new LinkedHashMap<>();
            result.put("count", sendCount);
            result.put("topic", last.topic());
            result.put("partition", last.partition());
            result.put("offset", last.offset());
            result.put("firstOffset", firstOffset);
            if (last.hasTimestamp()) {
                result.put("timestamp", last.timestamp());
            }
            return result;
        }
    }

    private static void applyHeaders(ProducerRecord<byte[], byte[]> record, Map<String, String> headers) {
        if (headers == null || headers.isEmpty()) {
            return;
        }
        for (Map.Entry<String, String> entry : headers.entrySet()) {
            if (StringUtils.isBlank(entry.getKey())) {
                continue;
            }
            byte[] headerValue = null;
            if (entry.getValue() != null) {
                headerValue = entry.getValue().getBytes(StandardCharsets.UTF_8);
            }
            record.headers().add(entry.getKey().trim(), headerValue);
        }
    }

    private static List<TopicPartition> resolveConsumePartitions(KafkaConsumer<byte[], byte[]> consumer, String topicName, Integer partition, Duration metadataTimeout) {
        List<org.apache.kafka.common.PartitionInfo> infos = consumer.partitionsFor(topicName, metadataTimeout);
        if (infos == null || infos.isEmpty()) {
            throw new IllegalArgumentException(KafkaKeys.ERR_TOPIC_NOT_FOUND);
        }
        List<TopicPartition> partitions = infos.stream().map(info -> new TopicPartition(topicName, info.partition())).toList();
        if (partitions.isEmpty()) {
            throw new IllegalArgumentException(KafkaKeys.ERR_TOPIC_NOT_FOUND);
        }
        if (partition == null) {
            return partitions;
        }
        TopicPartition selected = new TopicPartition(topicName, partition);
        if (!partitions.contains(selected)) {
            throw new IllegalArgumentException(KafkaKeys.ERR_PARTITION_NOT_FOUND);
        }
        return List.of(selected);
    }

    private static void seekConsume(KafkaConsumer<byte[], byte[]> consumer, List<TopicPartition> partitions, String mode, int limit, Duration metadataTimeout) {
        Map<TopicPartition, Long> begin = consumer.beginningOffsets(partitions, metadataTimeout);
        Map<TopicPartition, Long> end = consumer.endOffsets(partitions, metadataTimeout);
        if ("EARLIEST".equals(mode)) {
            begin.forEach(consumer::seek);
            return;
        }
        if ("LATEST".equals(mode)) {
            end.forEach(consumer::seek);
            return;
        }
        if ("GROUP".equals(mode)) {
            Map<TopicPartition, OffsetAndMetadata> committed = consumer.committed(new HashSet<>(partitions), metadataTimeout);
            for (TopicPartition tp : partitions) {
                OffsetAndMetadata meta = committed == null ? null : committed.get(tp);
                if (meta != null) {
                    consumer.seek(tp, meta.offset());
                } else {
                    consumer.seek(tp, begin.getOrDefault(tp, 0L));
                }
            }
            return;
        }
        // RECENT: last N messages across assigned partitions.
        long perPartition = Math.max(1L, (long) Math.ceil(limit * 1.0d / partitions.size()));
        for (TopicPartition tp : partitions) {
            long start = begin.getOrDefault(tp, 0L);
            long finish = end.getOrDefault(tp, start);
            long seekTo = Math.max(start, finish - perPartition);
            consumer.seek(tp, seekTo);
        }
    }

    private static Map<String, Object> toMessageRow(ConsumerRecord<byte[], byte[]> record) {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("topic", record.topic());
        row.put("partition", record.partition());
        row.put("offset", record.offset());
        if (record.timestamp() >= 0) {
            row.put("timestamp", record.timestamp());
        }
        if (record.timestampType() != null) {
            row.put("timestampType", record.timestampType().toString());
        } else {
            row.put("timestampType", null);
        }
        if (record.key() != null) {
            row.put("key", new String(record.key(), StandardCharsets.UTF_8));
        } else {
            row.put("key", null);
        }
        if (record.value() != null) {
            row.put("value", new String(record.value(), StandardCharsets.UTF_8));
        } else {
            row.put("value", null);
        }
        Map<String, String> headers = new LinkedHashMap<>();
        for (Header header : record.headers()) {
            if (header == null || StringUtils.isBlank(header.key())) {
                continue;
            }
            if (header.value() == null) {
                headers.put(header.key(), null);
            } else {
                headers.put(header.key(), new String(header.value(), StandardCharsets.UTF_8));
            }
        }
        row.put("headers", headers);
        return row;
    }

    private static Properties clientProperties(Connection connection) throws SQLException {
        return KafkaClientProps.unwrap(connection);
    }

    private static AdminClient unwrapAdmin(Connection connection) throws SQLException {
        return KafkaAdminUnwrap.unwrap(connection);
    }
}
