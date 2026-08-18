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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.clougence.clouddm.ds.kafka.execute.jdbc.KafkaKeys;
import com.clougence.clouddm.dsfamily.execute.AbstractMetadataProvider;
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

@Slf4j
public class KafkaMetaProviderDm extends AbstractMetadataProvider implements MetaDataService {

    public KafkaMetaProviderDm(Connection connection){
        super(connection);
    }

    @Override
    public String getVersion() {
        try (Connection conn = this.connectSupplier.eGet(); PreparedStatement ps = conn.prepareStatement("DESCRIBE CLUSTER")) {
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    if ("clusterId".equals(rs.getString("NAME"))) {
                        return rs.getString("VALUE");
                    }
                }
                return "kafka";
            }
        } catch (Exception e) {
            String msg = "getVersion failed, " + ExceptionUtils.getRootCauseMessage(e);
            log.error(msg, e);
            throw new RuntimeException(msg, e);
        }
    }

    public List<Value> selectSchemas() {
        RdbValue value = new RdbValue();
        value.setUmiType(UmiTypes.Schema);
        value.setValue(KafkaKeys.DEFAULT_SCHEMA);
        return Collections.singletonList(value);
    }

    public List<Value> selectTables() throws SQLException {
        try (Connection conn = this.connectSupplier.eGet(); PreparedStatement ps = conn.prepareStatement("SHOW TOPICS")) {
            try (ResultSet rs = ps.executeQuery()) {
                List<Value> result = new ArrayList<>();
                while (rs.next()) {
                    RdbValue value = new RdbValue();
                    value.setUmiType(UmiTypes.Table);
                    value.setValue(rs.getString("TOPIC"));
                    result.add(value);
                }
                result.sort((o1, o2) -> ((RdbValue) o1).getValue().compareTo(((RdbValue) o2).getValue()));
                return result;
            }
        }
    }

    public Value loadTable(String tableName) {
        RdbValue value = new RdbValue();
        value.setUmiType(UmiTypes.Table);
        value.setValue(tableName);
        return value;
    }

    @Override
    protected List<RdbTable> fetchTableByPart(Connection conn, String catalog, String schema, List<String> tabs) {
        return tabs.stream().map(tab -> {
            RdbTable rdbTable = new RdbTable();
            rdbTable.setName(tab);
            rdbTable.setSchema(schema);
            return rdbTable;
        }).collect(Collectors.toList());
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
