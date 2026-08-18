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
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.TopicDescription;

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
