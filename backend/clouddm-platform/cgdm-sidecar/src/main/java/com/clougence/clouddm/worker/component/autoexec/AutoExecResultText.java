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
package com.clougence.clouddm.worker.component.autoexec;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import com.clougence.clouddm.sdk.execute.resultset.echo.Result;
import com.clougence.clouddm.sdk.execute.resultset.echo.ResultCount;
import com.clougence.clouddm.sdk.execute.resultset.echo.ResultMessage;
import com.clougence.clouddm.sdk.execute.resultset.echo.ResultOut;
import com.clougence.clouddm.sdk.execute.resultset.echo.ResultSet;
import com.clougence.clouddm.sdk.execute.resultset.echo.ResultSetCount;
import com.clougence.clouddm.sdk.execute.resultset.echo.ResultSetMeta;
import com.clougence.clouddm.sdk.execute.resultset.echo.ResultSetRow;
import com.clougence.clouddm.sdk.execute.resultset.echo.ResultSetValue;
import com.clougence.clouddm.sdk.execute.session.MessageLevel;
import com.clougence.utils.StringUtils;

public final class AutoExecResultText {

    static final int MAX_PREVIEW_ROWS = 100;
    static final int MAX_CHARS        = 32 * 1024;
    static final int MAX_CELL_CHARS   = 64;

    private AutoExecResultText(){
    }

    public static String format(List<Result> results, long affectLine) {
        StringBuilder echo = new StringBuilder();
        List<String> columns = null;
        List<List<String>> previewRows = new ArrayList<>();
        int totalRows = 0;
        long resultSetCostMs = 0;
        boolean truncated = false;

        for (Result result : results) {
            if (result instanceof ResultMessage) {
                appendMessage(echo, (ResultMessage) result);
                continue;
            }
            if (result instanceof ResultOut) {
                appendOutParams(echo, (ResultOut) result);
                continue;
            }
            if (result instanceof ResultCount) {
                ResultCount count = (ResultCount) result;
                appendQueryOk(echo, count.getUpdateCount(), count.getCostTimeMs());
                continue;
            }
            if (result instanceof ResultSetMeta) {
                ResultSetMeta meta = (ResultSetMeta) result;
                columns = meta.getColumnList();
                resultSetCostMs = Math.max(resultSetCostMs, meta.getCostTimeMs());
                continue;
            }
            if (result instanceof ResultSet) {
                ResultSet resultSet = (ResultSet) result;
                resultSetCostMs = Math.max(resultSetCostMs, resultSet.getCostTimeMs());
                totalRows = Math.max(totalRows, resultSet.getFetchCount());
                truncated = appendPreviewRows(previewRows, resultSet, truncated);
                continue;
            }
            if (result instanceof ResultSetCount) {
                ResultSetCount setCount = (ResultSetCount) result;
                resultSetCostMs = Math.max(resultSetCostMs, setCount.getCostTimeMs());
                totalRows = Math.max(totalRows, setCount.getFetchCount());
            }
        }

        totalRows = Math.max(totalRows, previewRows.size());
        if (!previewRows.isEmpty() || columns != null) {
            appendAsciiTable(echo, columns, previewRows);
            if (truncated) {
                echo.append("... ").append(Math.max(0, totalRows - previewRows.size())).append(" more rows").append('\n');
            }
            appendRowsInSet(echo, totalRows, resultSetCostMs);
        } else if (echo.length() == 0) {
            appendQueryOk(echo, affectLine, 0);
        }

        return trimToMax(echo.toString().trim());
    }

    private static void appendMessage(StringBuilder echo, ResultMessage message) {
        if (message.getLevel() == MessageLevel.Error || StringUtils.isBlank(message.getMessage())) {
            return;
        }
        echo.append(message.getMessage()).append('\n');
    }

    private static void appendOutParams(StringBuilder echo, ResultOut resultOut) {
        Map<String, String> outParams = resultOut.getOutParams();
        if (outParams == null || outParams.isEmpty()) {
            return;
        }
        for (Map.Entry<String, String> entry : outParams.entrySet()) {
            echo.append(entry.getKey()).append(" = ").append(entry.getValue()).append('\n');
        }
    }

    private static boolean appendPreviewRows(List<List<String>> previewRows, ResultSet resultSet, boolean truncated) {
        List<ResultSetRow> rowSet = resultSet.getRowSet();
        if (rowSet == null || rowSet.isEmpty()) {
            return truncated;
        }
        for (ResultSetRow row : rowSet) {
            if (previewRows.size() >= MAX_PREVIEW_ROWS) {
                return true;
            }
            previewRows.add(rowValues(row));
        }
        return truncated;
    }

    private static List<String> rowValues(ResultSetRow row) {
        List<String> values = new ArrayList<>();
        if (row.getData() == null) {
            return values;
        }
        for (ResultSetValue cell : row.getData()) {
            if (cell == null || cell.getValue() == null) {
                values.add("NULL");
                continue;
            }
            values.add(cell.getValue());
        }
        return values;
    }

    private static void appendAsciiTable(StringBuilder echo, List<String> columns, List<List<String>> rows) {
        int columnCount = 0;
        if (columns != null) {
            columnCount = columns.size();
        }
        for (List<String> row : rows) {
            if (row.size() > columnCount) {
                columnCount = row.size();
            }
        }
        if (columnCount == 0) {
            return;
        }

        List<String> headers = new ArrayList<>();
        for (int i = 0; i < columnCount; i++) {
            if (columns != null && i < columns.size() && StringUtils.isNotBlank(columns.get(i))) {
                headers.add(columns.get(i));
            } else {
                headers.add("col" + (i + 1));
            }
        }

        int[] widths = new int[columnCount];
        for (int i = 0; i < columnCount; i++) {
            widths[i] = displayWidth(headers.get(i));
        }
        for (List<String> row : rows) {
            for (int i = 0; i < columnCount; i++) {
                String cell = "";
                if (i < row.size()) {
                    cell = clipCell(row.get(i));
                }
                int width = displayWidth(cell);
                if (width > widths[i]) {
                    widths[i] = width;
                }
            }
        }

        appendSeparator(echo, widths);
        appendRow(echo, headers, widths);
        appendSeparator(echo, widths);
        for (List<String> row : rows) {
            List<String> cells = new ArrayList<>();
            for (int i = 0; i < columnCount; i++) {
                if (i < row.size()) {
                    cells.add(clipCell(row.get(i)));
                } else {
                    cells.add("");
                }
            }
            appendRow(echo, cells, widths);
        }
        appendSeparator(echo, widths);
    }

    private static void appendSeparator(StringBuilder echo, int[] widths) {
        echo.append('+');
        for (int width : widths) {
            echo.append("-".repeat(width + 2)).append('+');
        }
        echo.append('\n');
    }

    private static void appendRow(StringBuilder echo, List<String> cells, int[] widths) {
        echo.append('|');
        for (int i = 0; i < widths.length; i++) {
            String cell = "";
            if (i < cells.size()) {
                cell = cells.get(i);
            }
            echo.append(' ').append(cell).append(" ".repeat(Math.max(0, widths[i] - displayWidth(cell)))).append(' ').append('|');
        }
        echo.append('\n');
    }

    private static void appendQueryOk(StringBuilder echo, long affectLine, long costTimeMs) {
        echo.append("Query OK, ").append(affectLine);
        if (affectLine == 1) {
            echo.append(" row affected");
        } else {
            echo.append(" rows affected");
        }
        appendCost(echo, costTimeMs);
        echo.append('\n');
    }

    private static void appendRowsInSet(StringBuilder echo, int totalRows, long costTimeMs) {
        if (totalRows == 0) {
            echo.append("Empty set");
        } else {
            echo.append(totalRows);
            if (totalRows == 1) {
                echo.append(" row in set");
            } else {
                echo.append(" rows in set");
            }
        }
        appendCost(echo, costTimeMs);
        echo.append('\n');
    }

    private static void appendCost(StringBuilder echo, long costTimeMs) {
        if (costTimeMs <= 0) {
            return;
        }
        echo.append(" (").append(String.format(Locale.ROOT, "%.2f", costTimeMs / 1000.0d)).append(" sec)");
    }

    private static String clipCell(String value) {
        if (value == null) {
            return "NULL";
        }
        if (value.length() <= MAX_CELL_CHARS) {
            return value.replace('\n', ' ').replace('\r', ' ');
        }
        return value.substring(0, MAX_CELL_CHARS - 3).replace('\n', ' ').replace('\r', ' ') + "...";
    }

    private static int displayWidth(String value) {
        if (value == null) {
            return 0;
        }
        return value.length();
    }

    private static String trimToMax(String text) {
        if (text.length() <= MAX_CHARS) {
            return text;
        }
        return text.substring(0, MAX_CHARS - 3) + "...";
    }
}
