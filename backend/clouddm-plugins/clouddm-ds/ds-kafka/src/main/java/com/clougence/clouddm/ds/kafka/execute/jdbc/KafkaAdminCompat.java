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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

class KafkaAdminCompat {

    static TopicDescription describeTopic(AdminClient admin, String topic, long timeout, TimeUnit unit) throws Exception {
        Object result = describeTopics(admin, topic);
        Map<?, ?> futures = topicFutures(result);
        Object future = futures.get(topic);
        if (future == null) {
            throw new SQLException("topic not found: " + topic);
        }
        return (TopicDescription) await(future, timeout, unit);
    }

    static Collection<?> listGroups(AdminClient admin, long timeout, TimeUnit unit) throws Exception {
        Method listConsumerGroups = findMethod(admin.getClass(), "listConsumerGroups");
        if (listConsumerGroups != null) {
            Object result = invoke(listConsumerGroups, admin);
            Object valid = invoke(result.getClass().getMethod("valid"), result);
            return (Collection<?>) await(valid, timeout, unit);
        }
        Object result = invoke(admin.getClass().getMethod("listGroups"), admin);
        Object all = invoke(result.getClass().getMethod("all"), result);
        return (Collection<?>) await(all, timeout, unit);
    }

    static Object describeGroup(AdminClient admin, String groupId, long timeout, TimeUnit unit) throws Exception {
        Object result = invoke(admin.getClass().getMethod("describeConsumerGroups", Collection.class), admin, Collections.singletonList(groupId));
        Object allFuture = invoke(result.getClass().getMethod("all"), result);
        Map<?, ?> values = (Map<?, ?>) await(allFuture, timeout, unit);
        Object group = values.get(groupId);
        if (group == null) {
            throw new SQLException("consumer group not found: " + groupId);
        }
        return group;
    }

    @SuppressWarnings("unchecked")
    static Map<TopicPartition, OffsetAndMetadata> listGroupOffsets(AdminClient admin, String groupId, long timeout, TimeUnit unit) throws Exception {
        Method byGroup = findMethod(admin.getClass(), "listConsumerGroupOffsets", String.class);
        if (byGroup != null) {
            Object result = invoke(byGroup, admin, groupId);
            Object future = invoke(result.getClass().getMethod("partitionsToOffsetAndMetadata"), result);
            return (Map<TopicPartition, OffsetAndMetadata>) await(future, timeout, unit);
        }
        throw new SQLException("listConsumerGroupOffsets is not supported by current Kafka client.");
    }

    static void deleteGroup(AdminClient admin, String groupId, long timeout, TimeUnit unit) throws Exception {
        Object result = invoke(admin.getClass().getMethod("deleteConsumerGroups", Collection.class), admin, Collections.singletonList(groupId));
        await(invoke(result.getClass().getMethod("all"), result), timeout, unit);
    }

    static void alterGroupOffsets(AdminClient admin, String groupId, Map<TopicPartition, OffsetAndMetadata> offsets, long timeout, TimeUnit unit) throws Exception {
        Object result = invoke(admin.getClass().getMethod("alterConsumerGroupOffsets", String.class, Map.class), admin, groupId, offsets);
        await(invoke(result.getClass().getMethod("all"), result), timeout, unit);
    }

    @SuppressWarnings("unchecked")
    static Map<TopicPartition, Object> listOffsets(AdminClient admin, Map<TopicPartition, OffsetSpec> specs, long timeout, TimeUnit unit) throws Exception {
        Object result = invoke(admin.getClass().getMethod("listOffsets", Map.class), admin, specs);
        Object future = invoke(result.getClass().getMethod("all"), result);
        return (Map<TopicPartition, Object>) await(future, timeout, unit);
    }

    static long listOffsetValue(Object listOffsetsResultInfo) throws Exception {
        Object offset = invoke(listOffsetsResultInfo.getClass().getMethod("offset"), listOffsetsResultInfo);
        return ((Number) offset).longValue();
    }

    static String groupState(Object description) {
        try {
            Object state = invoke(description.getClass().getMethod("state"), description);
            return state == null ? "" : String.valueOf(state);
        } catch (Exception e) {
            return "";
        }
    }

    static int groupMembers(Object description) {
        try {
            Collection<?> members = (Collection<?>) invoke(description.getClass().getMethod("members"), description);
            return members == null ? 0 : members.size();
        } catch (Exception e) {
            return 0;
        }
    }

    static String listingId(Object listing) throws Exception {
        return String.valueOf(invoke(listing.getClass().getMethod("groupId"), listing));
    }

    static String listingState(Object listing) {
        try {
            Object state = invoke(listing.getClass().getMethod("state"), listing);
            if (state == null) {
                return "";
            }
            if (state instanceof Optional) {
                Optional<?> opt = (Optional<?>) state;
                if (opt.isEmpty()) {
                    return "";
                }
                return String.valueOf(opt.get());
            }
            return String.valueOf(state);
        } catch (Exception e) {
            return "";
        }
    }

    static Map<TopicPartition, OffsetSpec> offsetSpecs(Collection<TopicPartition> partitions, OffsetSpec spec) {
        Map<TopicPartition, OffsetSpec> specs = new LinkedHashMap<>();
        for (TopicPartition partition : partitions) {
            specs.put(partition, spec);
        }
        return specs;
    }

    private static Object describeTopics(AdminClient admin, String topic) throws Exception {
        Method byNames = findMethod(admin.getClass(), "describeTopics", Collection.class);
        if (byNames != null) {
            return invoke(byNames, admin, Collections.singletonList(topic));
        }
        Class<?> topicCollectionClass = Class.forName("org.apache.kafka.clients.admin.TopicCollection");
        Object names = invoke(topicCollectionClass.getMethod("ofTopicNames", Collection.class), null, Collections.singletonList(topic));
        return invoke(admin.getClass().getMethod("describeTopics", topicCollectionClass), admin, names);
    }

    private static Map<?, ?> topicFutures(Object describeResult) throws Exception {
        Method topicNameValues = findMethod(describeResult.getClass(), "topicNameValues");
        if (topicNameValues != null) {
            return (Map<?, ?>) invoke(topicNameValues, describeResult);
        }
        return (Map<?, ?>) invoke(describeResult.getClass().getMethod("values"), describeResult);
    }

    private static Object await(Object future, long timeout, TimeUnit unit) throws Exception {
        return invoke(future.getClass().getMethod("get", long.class, TimeUnit.class), future, timeout, unit);
    }

    private static Method findMethod(Class<?> type, String name, Class<?>... parameterTypes) {
        try {
            return type.getMethod(name, parameterTypes);
        } catch (NoSuchMethodException e) {
            return null;
        }
    }

    private static Object invoke(Method method, Object target, Object... args) throws Exception {
        try {
            return method.invoke(target, args);
        } catch (InvocationTargetException e) {
            Throwable cause = e.getCause();
            if (cause instanceof Exception) {
                throw (Exception) cause;
            }
            throw e;
        }
    }
}
