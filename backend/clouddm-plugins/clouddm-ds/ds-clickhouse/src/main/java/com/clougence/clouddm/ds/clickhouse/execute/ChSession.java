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
package com.clougence.clouddm.ds.clickhouse.execute;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.atomic.AtomicReference;

import com.clougence.clouddm.base.metadata.ds.DataSourceConfig;
import com.clougence.clouddm.sdk.execute.session.QueryRequest;
import com.clougence.clouddm.sdk.execute.session.ResultBuilder;
import com.clougence.clouddm.sdk.execute.session.rdb.DefaultRdbSession;
import com.clougence.drivers.DsObject;

import lombok.extern.slf4j.Slf4j;

/**
 * @author Ekko 2022/11/03 16:48
 */
@Slf4j
public class ChSession extends DefaultRdbSession {

    private final AtomicReference<Statement> currentStatement = new AtomicReference<>();

    public ChSession(String newSessionId, DataSourceConfig dsConfig, DsObject<Connection> dsObject){
        super(newSessionId, dsConfig, dsObject, new ChHooks(dsConfig));
    }

    @Override
    protected Statement createStatement(Connection conn, QueryRequest query) throws SQLException {
        Statement statement = super.createStatement(conn, query);
        this.currentStatement.set(statement);
        return statement;
    }

    @Override
    public void killCurrentQuery() throws Exception {
        Statement statement = this.currentStatement.get();
        if (statement != null && !statement.isClosed()) {
            // clickhouse-jdbc cancel uses the real query_id:
            // KILL QUERY WHERE query_id = '...'
            try {
                statement.cancel();
                return;
            } catch (SQLException e) {
                log.warn("ClickHouse Statement.cancel failed, fallback to killProcess: {}", e.getMessage());
            }
        }
        try {
            super.killCurrentQuery();
        } catch (Exception e) {
            log.warn("ClickHouse killProcess failed: {}", e.getMessage());
        }
        // Last resort: close the connection so blocked JDBC can exit and autoexec can leave EXECUTING.
        log.warn("ClickHouse cancel fallback: closing connection to unblock executing query");
        doClose();
    }

    @Override
    protected void beforeQueryRequest(long beginTime, QueryRequest query, ResultBuilder builder) throws SQLException {
        query.getResultConf().setFetchMoreResult(false);
        super.beforeQueryRequest(beginTime, query, builder);
    }

    @Override
    public long getUpdateCount(Statement ps) throws SQLException {
        return ps.getUpdateCount();
    }
}
