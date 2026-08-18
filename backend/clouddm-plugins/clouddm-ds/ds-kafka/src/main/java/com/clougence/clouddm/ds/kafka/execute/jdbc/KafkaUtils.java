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
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.clougence.drivers.adapter.AdapterRequest;
import com.clougence.drivers.adapter.AdapterResultCursor;
import com.clougence.drivers.adapter.AdapterType;
import com.clougence.drivers.adapter.JdbcColumn;
import com.clougence.utils.future.CgFuture;

class KafkaUtils {

    static final JdbcColumn TOPIC     = new JdbcColumn("TOPIC", AdapterType.String, "", "", "");
    static final JdbcColumn GROUP_ID  = new JdbcColumn("GROUP_ID", AdapterType.String, "", "", "");
    static final JdbcColumn STATE     = new JdbcColumn("STATE", AdapterType.String, "", "", "");
    static final JdbcColumn NAME      = new JdbcColumn("NAME", AdapterType.String, "", "", "");
    static final JdbcColumn VALUE     = new JdbcColumn("VALUE", AdapterType.String, "", "", "");
    static final JdbcColumn PARTITION = new JdbcColumn("PARTITION", AdapterType.Int, "", "", "");
    static final JdbcColumn LEADER    = new JdbcColumn("LEADER", AdapterType.Int, "", "", "");
    static final JdbcColumn REPLICAS  = new JdbcColumn("REPLICAS", AdapterType.String, "", "", "");
    static final JdbcColumn ISR       = new JdbcColumn("ISR", AdapterType.String, "", "", "");
    static final JdbcColumn OFFSET    = new JdbcColumn("OFFSET", AdapterType.Long, "", "", "");
    static final JdbcColumn TIMESTAMP = new JdbcColumn("TIMESTAMP", AdapterType.Long, "", "", "");
    static final JdbcColumn KEY       = new JdbcColumn("KEY", AdapterType.String, "", "", "");
    static final JdbcColumn MSG_VALUE = new JdbcColumn("VALUE", AdapterType.String, "", "", "");

    static CgFuture<?> completed(CgFuture<Object> sync) {
        sync.completed(true);
        return sync;
    }

    static CgFuture<?> failed(CgFuture<Object> sync, Exception e) {
        sync.failed(e);
        return sync;
    }

    static AdapterResultCursor rows(AdapterRequest request, List<JdbcColumn> columns, List<Map<String, Object>> rows) throws SQLException {
        AdapterResultCursor cursor = new AdapterResultCursor(request, columns);
        long maxRows = request.getMaxRows();
        int count = 0;
        for (Map<String, Object> row : rows) {
            if (maxRows > 0 && count >= maxRows) {
                break;
            }
            cursor.pushData(row);
            count++;
        }
        cursor.pushFinish();
        return cursor;
    }

    static Map<String, Object> row(Object... kv) {
        Map<String, Object> data = new LinkedHashMap<>();
        for (int i = 0; i + 1 < kv.length; i += 2) {
            data.put(String.valueOf(kv[i]), kv[i + 1]);
        }
        return data;
    }

    static List<JdbcColumn> columns(JdbcColumn... cols) {
        List<JdbcColumn> list = new ArrayList<>();
        for (JdbcColumn col : cols) {
            list.add(col);
        }
        return list;
    }
}
