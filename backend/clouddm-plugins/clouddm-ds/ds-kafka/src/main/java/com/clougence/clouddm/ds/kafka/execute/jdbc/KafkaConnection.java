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

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

import org.apache.kafka.clients.admin.AdminClient;

import com.clougence.drivers.adapter.AdapterConnManager;
import com.clougence.drivers.adapter.AdapterConnection;
import com.clougence.drivers.adapter.AdapterReceive;
import com.clougence.drivers.adapter.AdapterRequest;
import com.clougence.utils.ExceptionUtils;
import com.clougence.utils.future.CgFuture;
import com.clougence.utils.future.CgFutureObj;

public class KafkaConnection extends AdapterConnection {

    private final Connection          owner;
    private final AdminClient         admin;
    private final Map<String, String> dsConfig;
    private final String              bootstrap;
    private String                    schema = KafkaKeys.DEFAULT_SCHEMA;

    KafkaConnection(Connection owner, AdminClient admin, Map<String, String> dsConfig, String bootstrap, String jdbcUrl){
        super(jdbcUrl, dsConfig.get(KafkaKeys.USERNAME));
        this.owner = owner;
        this.admin = admin;
        this.dsConfig = dsConfig;
        this.bootstrap = bootstrap;
    }

    public AdminClient getAdmin() { return this.admin; }

    public Map<String, String> getDsConfig() { return this.dsConfig; }

    public String getBootstrap() { return this.bootstrap; }

    @Override
    public String getCatalog() { return this.getSchema(); }

    @Override
    public void setCatalog(String catalog) {
        this.setSchema(catalog);
    }

    @Override
    public String getSchema() { return this.schema; }

    @Override
    public void setSchema(String schema) {
        if (schema != null && !schema.isEmpty()) {
            this.schema = schema;
        }
    }

    @Override
    public AdapterRequest newRequest(String sql) {
        return new KafkaRequest(sql);
    }

    @Override
    protected <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface == KafkaConnection.class) {
            return (T) this;
        } else if (iface == AdminClient.class) {
            return (T) this.admin;
        } else {
            return super.unwrap(iface);
        }
    }

    public void killDriverConnection(String connID) throws SQLException {
        KafkaConnection conn = (KafkaConnection) AdapterConnManager.getConnection(connID);
        if (conn != null) {
            try {
                conn.close();
            } catch (Throwable e) {
                Throwable ee = ExceptionUtils.getRootCause(e);
                if (ee instanceof SQLException) {
                    throw (SQLException) ee;
                }
                throw new SQLException(e);
            }
        }
    }

    @Override
    public synchronized void doRequest(AdapterRequest request, AdapterReceive receive) throws SQLException {
        KafkaCommand command = KafkaCommandParser.parse(((KafkaRequest) request).getCommandBody());
        CgFuture<Object> sync = new CgFutureObj<>();
        KafkaDistributeCall.exec(sync, this, command, request, receive);
        sync.await();
        receive.responseFinish(request);
    }

    @Override
    public void cancelRequest() {
        throw new UnsupportedOperationException("cancelRequest not support.");
    }

    @Override
    protected void doClose() throws IOException {
        this.admin.close();
    }
}
