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

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.TimeZone;

import org.bson.BsonTimestamp;
import org.bson.Document;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;

/**
 * Formats mongosh-style text for rs.print* helpers.
 */
final class MongoShellPrintFormatter {

    private static final int SECONDARY_STATE = 2;

    private MongoShellPrintFormatter() {
    }

    static String printSecondaryReplicationInfo(MongoClient client) {
        Document status = client.getDatabase("admin").runCommand(new Document("replSetGetStatus", 1));
        List<Document> members = status.getList("members", Document.class);
        if (members == null || members.isEmpty()) {
            return "no members found in replSetGetStatus";
        }

        Date primaryOptime = null;
        for (Document member : members) {
            Integer state = member.getInteger("state");
            if (state != null && state == 1) {
                primaryOptime = member.getDate("optimeDate");
                break;
            }
        }

        StringBuilder out = new StringBuilder();
        for (Document member : members) {
            Integer state = member.getInteger("state");
            if (state == null || state != SECONDARY_STATE) {
                continue;
            }
            String name = member.getString("name");
            Date syncedTo = member.getDate("optimeDate");
            out.append("source: ").append(name == null ? "" : name).append('\n');
            out.append("\tsyncedTo: ").append(formatShellDate(syncedTo)).append('\n');
            if (primaryOptime != null && syncedTo != null) {
                long secs = (primaryOptime.getTime() - syncedTo.getTime()) / 1000L;
                long hrs = secs / 3600L;
                out.append('\t').append(secs).append(" secs (").append(hrs).append(" hrs) behind the primary \n");
            } else {
                out.append("\t(no primary optime available)\n");
            }
        }
        if (out.length() == 0) {
            return "no secondaries found";
        }
        // trim trailing newline to match typical shell print, keep final newline-free last line content as mongosh
        if (out.charAt(out.length() - 1) == '\n') {
            out.setLength(out.length() - 1);
        }
        return out.toString();
    }

    static String printReplicationInfo(MongoClient client) {
        MongoCollection<Document> oplog = client.getDatabase("local").getCollection("oplog.rs");
        Document stats = client.getDatabase("local").runCommand(new Document("collStats", "oplog.rs"));

        Number maxSize = stats.get("maxSize", Number.class);
        double logSizeMB = 0;
        if (maxSize != null) {
            logSizeMB = maxSize.doubleValue() / (1024d * 1024d);
        }

        Document first = oplog.find().sort(new Document("$natural", 1)).limit(1).first();
        Document last = oplog.find().sort(new Document("$natural", -1)).limit(1).first();
        Date tFirst = oplogEventTime(first);
        Date tLast = oplogEventTime(last);
        Date now = new Date();

        long timeDiffSecs = 0;
        if (tFirst != null && tLast != null) {
            timeDiffSecs = (tLast.getTime() - tFirst.getTime()) / 1000L;
        }
        double timeDiffHours = Math.round(timeDiffSecs / 36.0) / 100.0;

        StringBuilder out = new StringBuilder();
        out.append("configured oplog size:   ").append(formatNumber(logSizeMB)).append(" MB\n");
        out.append("log length start to end: ").append(timeDiffSecs).append(" secs (").append(formatNumber(timeDiffHours)).append(" hrs)\n");
        out.append("oplog first event time:  ").append(formatShellDate(tFirst)).append('\n');
        out.append("oplog last event time:   ").append(formatShellDate(tLast)).append('\n');
        out.append("now:                     ").append(formatShellDate(now));
        return out.toString();
    }

    private static Date oplogEventTime(Document doc) {
        if (doc == null) {
            return null;
        }
        Object ts = doc.get("ts");
        if (ts instanceof Date date) {
            return date;
        }
        if (ts instanceof BsonTimestamp bsonTimestamp) {
            return new Date(bsonTimestamp.getTime() * 1000L);
        }
        return null;
    }

    private static String formatShellDate(Date date) {
        if (date == null) {
            return "null";
        }
        SimpleDateFormat sdf = new SimpleDateFormat("EEE MMM dd yyyy HH:mm:ss 'GMT'Z (z)", Locale.ENGLISH);
        sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
        return sdf.format(date);
    }

    private static String formatNumber(double value) {
        if (Math.rint(value) == value) {
            return String.valueOf((long) value);
        }
        return String.valueOf(value);
    }
}
