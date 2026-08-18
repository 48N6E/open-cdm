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

import com.clougence.clouddm.base.metadata.ds.ColMetaData;
import com.clougence.clouddm.dsfamily.execute.AbstractColReader;
import com.clougence.clouddm.sdk.execute.session.result.fetcher.ValueFetcher;
import com.clougence.drivers.adapter.AdapterType;
import com.clougence.utils.StringUtils;

public class KafkaColReader extends AbstractColReader {

    @Override
    public ValueFetcher readColumn(String col, ColMetaData colMetaData) {
        String colType = StringUtils.defaultString(colMetaData.getColumnType(), "");
        switch (colType) {
            case AdapterType.Int:
                return INTEGER_VALUE_FETCHER;
            case AdapterType.Long:
                return LONG_VALUE_FETCHER;
            case AdapterType.String:
                return STRING_AS_READER_FETCHER;
            default:
                return STRING_AS_READER_FETCHER;
        }
    }
}
