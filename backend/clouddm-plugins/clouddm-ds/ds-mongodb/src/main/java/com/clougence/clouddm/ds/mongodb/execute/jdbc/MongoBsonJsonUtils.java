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
package com.clougence.clouddm.ds.mongodb.execute.jdbc;

import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.bson.Document;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;

import com.clougence.utils.JsonUtils;
import com.fasterxml.jackson.databind.JsonNode;

final class MongoBsonJsonUtils {

    private static final JsonWriterSettings SHELL_JSON  = JsonWriterSettings.builder()
                                                                           .outputMode(JsonMode.SHELL)
                                                                           .indent(true)
                                                                           .build();
    private static final JsonWriterSettings STRICT_JSON = JsonWriterSettings.builder()
                                                                           .outputMode(JsonMode.STRICT)
                                                                           .build();

    private MongoBsonJsonUtils() {
    }

    static String toShellJson(Document document) {
        if (document == null) {
            return "null";
        }
        return document.toJson(SHELL_JSON);
    }

    static Object toJdbcValue(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof String || value instanceof Number || value instanceof Boolean) {
            return value;
        }
        // mongosh-style: never Jackson-serialize ObjectId (would become {timestamp,date})
        if (value instanceof ObjectId objectId) {
            return "ObjectId(\"" + objectId.toHexString() + "\")";
        }
        if (value instanceof Date date) {
            return "ISODate(\"" + date.toInstant().toString() + "\")";
        }
        if (value instanceof Decimal128 decimal128) {
            return decimal128.bigDecimalValue();
        }
        if (value instanceof Document document) {
            Map<String, Object> map = new LinkedHashMap<>();
            document.forEach((k, v) -> map.put(k, toJdbcValue(v)));
            return JsonUtils.toJson(map);
        }
        if (value instanceof List<?> list) {
            List<Object> converted = new ArrayList<>(list.size());
            for (Object item : list) {
                converted.add(toJdbcValue(item));
            }
            return JsonUtils.toJson(converted);
        }
        // other BSON types: keep Extended JSON text, never re-parse back to Java beans
        String strict = new Document("v", value).toJson(STRICT_JSON);
        try {
            JsonNode node = JsonUtils.defaultObjectMapper().readTree(strict).get("v");
            if (node == null || node.isNull()) {
                return null;
            }
            if (node.isTextual()) {
                return node.asText();
            }
            if (node.isNumber()) {
                return node.numberValue();
            }
            if (node.isBoolean()) {
                return node.booleanValue();
            }
            return node.toString();
        } catch (Exception e) {
            return strict;
        }
    }
}
