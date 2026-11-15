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

import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

import com.clougence.drivers.adapter.AdapterConnection;

/**
 * Resolve Kafka client Properties across JDBC proxies and dual ClassLoaders.
 */
public final class KafkaClientProps {

    private KafkaClientProps(){
    }

    public static Properties unwrap(Connection connection) throws SQLException {
        if (connection == null) {
            throw new SQLException("failed to unwrap Kafka client properties from null connection");
        }
        try {
            AdapterConnection adapter = connection.unwrap(AdapterConnection.class);
            if (adapter == null) {
                throw new SQLException("failed to unwrap AdapterConnection from " + connection.getClass().getName());
            }
            Method getClientProperties = adapter.getClass().getMethod("getClientProperties");
            Object props = getClientProperties.invoke(adapter);
            if (!(props instanceof Properties)) {
                throw new SQLException("Kafka client properties unavailable from " + adapter.getClass().getName());
            }
            return (Properties) props;
        } catch (ReflectiveOperationException e) {
            throw new SQLException("failed to unwrap Kafka client properties from " + connection.getClass().getName(), e);
        }
    }
}
