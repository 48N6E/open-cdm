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

import org.apache.kafka.clients.admin.AdminClient;

import com.clougence.drivers.adapter.AdapterConnection;

/**
 * Resolve {@link AdminClient} across JDBC proxies and dual plugin/driver ClassLoaders.
 * {@code Connection.unwrap(AdminClient.class)} fails when the caller's {@code AdminClient.class}
 * is not the same Class identity as the one linked into {@link KafkaConnection}.
 */
public final class KafkaAdminUnwrap {

    private static final String ADMIN_CLIENT_NAME = "org.apache.kafka.clients.admin.AdminClient";

    private KafkaAdminUnwrap(){
    }

    public static AdminClient unwrap(Connection connection) throws SQLException {
        if (connection == null) {
            throw new SQLException("failed to unwrap AdminClient from null connection");
        }

        Object admin = digAdminClient(connection);
        if (admin == null) {
            throw new SQLException("failed to unwrap AdminClient from " + connection.getClass().getName());
        }
        if (admin instanceof AdminClient) {
            return (AdminClient) admin;
        }
        throw new SQLException("AdminClient ClassLoader mismatch from " + connection.getClass().getName() //
                               + ", got " + admin.getClass().getName() + " @" + admin.getClass().getClassLoader());
    }

    static boolean isAdminClientType(Class<?> iface) {
        return iface != null && ADMIN_CLIENT_NAME.equals(iface.getName());
    }

    private static Object digAdminClient(Connection connection) {
        try {
            AdapterConnection adapter = connection.unwrap(AdapterConnection.class);
            if (adapter == null) {
                return null;
            }
            Method getAdminClient = adapter.getClass().getMethod("getAdminClient");
            return getAdminClient.invoke(adapter);
        } catch (ReflectiveOperationException | SQLException e) {
            return null;
        }
    }
}
