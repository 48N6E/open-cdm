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
package com.clougence.clouddm.ds.kafka.execute;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.clougence.clouddm.ds.kafka.execute.jdbc.KafkaKeys;
import com.clougence.clouddm.dsfamily.execute.AbstractMetadataProvider;
import com.clougence.drivers.adapter.AdapterConnection;
import com.clougence.schema.metadata.MetaDataService;
import com.clougence.schema.umi.special.rdb.RdbColumn;
import com.clougence.schema.umi.special.rdb.RdbForeignKey;
import com.clougence.schema.umi.special.rdb.RdbIndex;
import com.clougence.schema.umi.special.rdb.RdbTable;
import com.clougence.schema.umi.special.rdb.RdbValue;
import com.clougence.schema.umi.struts.UmiConstraint;
import com.clougence.schema.umi.struts.UmiTypes;
import com.clougence.schema.umi.struts.Value;
import com.clougence.schema.umi.struts.constraint.ConstraintObject;
import com.clougence.utils.ExceptionUtils;

import lombok.extern.slf4j.Slf4j;

/**
 * Plugin-CL metadata facade. Real AdminClient work runs in {@code KafkaMetaAdmin} on the driver ClassLoader.
 */
@Slf4j
public class KafkaMetaProviderDm extends AbstractMetadataProvider implements MetaDataService {

    private static final String META_ADMIN = "com.clougence.clouddm.ds.kafka.execute.jdbc.KafkaMetaAdmin";

    public KafkaMetaProviderDm(Connection connection){
        super(connection);
    }

    @Override
    public String getVersion() {
        try (Connection conn = this.connectSupplier.eGet()) {
            return (String) invoke(conn, "getVersion", new Class<?>[] { Connection.class }, conn);
        } catch (Exception e) {
            String msg = "getVersion failed, " + ExceptionUtils.getRootCauseMessage(e);
            log.error(msg, e);
            throw new RuntimeException(msg, e);
        }
    }

    public List<Value> selectSchemas() throws SQLException {
        List<Value> result = new ArrayList<>(3);
        result.add(schemaValue(KafkaKeys.SCHEMA_TOPIC));
        result.add(schemaValue(KafkaKeys.SCHEMA_CONSUMER_GROUP));
        result.add(schemaValue(KafkaKeys.SCHEMA_ENDPOINT));
        return result;
    }

    private static RdbValue schemaValue(String name) {
        RdbValue value = new RdbValue();
        value.setValue(name);
        value.setUmiType(UmiTypes.Schema);
        return value;
    }

    @SuppressWarnings("unchecked")
    public List<Value> selectTopics(String pattern) throws SQLException {
        try (Connection conn = this.connectSupplier.eGet()) {
            return (List<Value>) invoke(conn, "selectTopics", new Class<?>[] { Connection.class, String.class }, conn, pattern);
        } catch (SQLException e) {
            throw e;
        } catch (Exception e) {
            throw toSqlException("list topics failed", e);
        }
    }

    @SuppressWarnings("unchecked")
    public List<Value> selectConsumerGroups(String pattern) throws SQLException {
        try (Connection conn = this.connectSupplier.eGet()) {
            return (List<Value>) invoke(conn, "selectConsumerGroups", new Class<?>[] { Connection.class, String.class }, conn, pattern);
        } catch (SQLException e) {
            throw e;
        } catch (Exception e) {
            throw toSqlException("list consumer groups failed", e);
        }
    }

    @SuppressWarnings("unchecked")
    public List<Value> selectBrokers(String pattern) throws SQLException {
        try (Connection conn = this.connectSupplier.eGet()) {
            return (List<Value>) invoke(conn, "selectBrokers", new Class<?>[] { Connection.class, String.class }, conn, pattern);
        } catch (SQLException e) {
            throw e;
        } catch (Exception e) {
            throw toSqlException("list brokers failed", e);
        }
    }

    public Value loadTopic(String topicName) throws SQLException {
        try (Connection conn = this.connectSupplier.eGet()) {
            return (Value) invoke(conn, "loadTopic", new Class<?>[] { Connection.class, String.class }, conn, topicName);
        } catch (SQLException e) {
            throw e;
        } catch (Exception e) {
            throw toSqlException("describe topic failed", e);
        }
    }

    public Value loadConsumerGroup(String groupId) throws SQLException {
        try (Connection conn = this.connectSupplier.eGet()) {
            return (Value) invoke(conn, "loadConsumerGroup", new Class<?>[] { Connection.class, String.class }, conn, groupId);
        } catch (SQLException e) {
            throw e;
        } catch (Exception e) {
            throw toSqlException("describe consumer group failed", e);
        }
    }

    public Value loadBroker(String brokerName) throws SQLException {
        try (Connection conn = this.connectSupplier.eGet()) {
            return (Value) invoke(conn, "loadBroker", new Class<?>[] { Connection.class, String.class }, conn, brokerName);
        } catch (SQLException e) {
            throw e;
        } catch (Exception e) {
            throw toSqlException("describe broker failed", e);
        }
    }

    public void testConnect() throws SQLException {
        try (Connection conn = this.connectSupplier.eGet()) {
            invoke(conn, "testConnect", new Class<?>[] { Connection.class }, conn);
        } catch (SQLException e) {
            throw e;
        } catch (Exception e) {
            throw toSqlException("testConnect failed", e);
        }
    }

    private static Object invoke(Connection connection, String methodName, Class<?>[] parameterTypes, Object... args) throws Exception {
        try {
            ClassLoader driverCl = resolveDriverClassLoader(connection);
            Class<?> metaType = Class.forName(META_ADMIN, true, driverCl);
            Method method = metaType.getMethod(methodName, parameterTypes);
            return method.invoke(null, args);
        } catch (InvocationTargetException e) {
            Throwable cause = e.getCause() == null ? e : e.getCause();
            if (cause instanceof Exception) {
                throw (Exception) cause;
            }
            throw new Exception(cause);
        }
    }

    private static ClassLoader resolveDriverClassLoader(Connection connection) throws SQLException {
        AdapterConnection adapter = connection.unwrap(AdapterConnection.class);
        if (adapter != null && adapter.getClass().getClassLoader() != null) {
            return adapter.getClass().getClassLoader();
        }
        ClassLoader cl = connection.getClass().getClassLoader();
        if (cl == null) {
            cl = Thread.currentThread().getContextClassLoader();
        }
        return cl;
    }

    private static SQLException toSqlException(String message, Exception e) {
        String msg = message + ", " + ExceptionUtils.getRootCauseMessage(e);
        log.error(msg, e);
        return new SQLException(msg, e);
    }

    @Override
    protected List<RdbTable> fetchTableByPart(Connection conn, String catalog, String schema, List<String> tabs) {
        return Collections.emptyList();
    }

    @Override
    protected List<RdbTable> fetchViewByPart(Connection conn, String catalog, String schema, List<String> tabs) {
        return Collections.emptyList();
    }

    @Override
    protected List<RdbTable> fetchMaterializedByPart(Connection conn, String catalog, String schema, List<String> tabs) {
        return Collections.emptyList();
    }

    @Override
    protected Map<String, List<RdbColumn>> fetchViewColumns(Connection conn, String catalog, String schema, List<String> tabs) {
        return Collections.emptyMap();
    }

    @Override
    protected Map<String, List<RdbColumn>> fetchTableColumns(Connection conn, String catalog, String schema, List<String> tabs) {
        return Collections.emptyMap();
    }

    @Override
    protected Map<String, List<ConstraintObject>> fetchTableConstraints(Connection conn, String catalog, String schema, List<String> tabs) {
        return Collections.emptyMap();
    }

    @Override
    protected Map<String, Map<String, UmiConstraint>> fetchPrimaryUnique(Connection conn, String catalog, String schema, List<String> tabs) {
        return Collections.emptyMap();
    }

    @Override
    protected Map<String, List<RdbForeignKey>> fetchForeignKeys(Connection conn, String catalog, String schema, List<String> tabs) {
        return Collections.emptyMap();
    }

    @Override
    protected Map<String, List<RdbIndex>> fetchIndexes(Connection conn, String catalog, String schema, List<String> tabs) {
        return Collections.emptyMap();
    }
}
