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

import com.clougence.clouddm.base.metadata.ds.DataSourceConfig;
import com.clougence.clouddm.dsfamily.execute.RdbSessionSpi;
import com.clougence.clouddm.sdk.execute.session.QueryRequest;

/**
 * ClickHouse HTTP/JDBC protocol only returns one result set per statement.
 * Keeping {@code fetchMoreResult=false} avoids spinning in
 * {@code DefaultRdbSession#handleRs} when {@link ChSession#getUpdateCount}
 * maps JDBC update count {@code 0} to {@code 1}.
 */
public class ChSessionSpi extends RdbSessionSpi {

    @Override
    public QueryRequest createQueryRequest(DataSourceConfig dsConfig) {
        QueryRequest request = super.createQueryRequest(dsConfig);
        request.getResultConf().setFetchMoreResult(false);
        return request;
    }
}
