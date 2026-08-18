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
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import com.clougence.clouddm.dsfamily.execute.AbstractRdbUmiService;
import com.clougence.schema.umi.service.RdbUmiServiceDm;
import com.clougence.schema.umi.struts.UmiTypes;
import com.clougence.schema.umi.struts.Value;
import com.clougence.utils.StringUtils;

public class KafkaUmiServiceDm extends AbstractRdbUmiService<KafkaMetaProviderDm> implements RdbUmiServiceDm {

    public KafkaUmiServiceDm(Connection connection){
        super(() -> new KafkaMetaProviderDm(connection));
    }

    @Override
    public List<Value> listLevels(List<UmiTypes> levels, Map<UmiTypes, Object> levelsParam) throws SQLException {
        if (levels.isEmpty()) {
            return this.metadataSupplier.eGet().selectSchemas();
        }
        throw new UnsupportedOperationException("listLevels[" + StringUtils.join(levels.toArray(), ",") + "] Unsupported.");
    }

    @Override
    public List<Value> listLeaf(Map<UmiTypes, Object> levelsParam, UmiTypes leafType, String pattern) throws SQLException {
        if (leafType == UmiTypes.Table) {
            return this.metadataSupplier.eGet().selectTables();
        }
        throw new UnsupportedOperationException("listLeaf of " + leafType + " Unsupported.");
    }

    @Override
    public Value detailLeaf(Map<UmiTypes, Object> levelsParam, UmiTypes leafType, String leafName) throws SQLException {
        if (leafType == UmiTypes.Table) {
            return this.metadataSupplier.eGet().loadTable(leafName);
        }
        throw new UnsupportedOperationException("detailLeaf of " + leafType + " Unsupported.");
    }
}
