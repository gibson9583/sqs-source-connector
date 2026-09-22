/* SPDX-License-Identifier: MPL-2.0 */
package io.github.gibson9583.sqs;

import java.util.Base64;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;
import com.mirth.connect.connectors.sqs.SqsMessageAttribute;

final class SqsPanelValidation {
    private static final Pattern EXPRESSION = Pattern.compile("\\$!?\\{[^}]+\\}|\\$!?[A-Za-z_][\\w.]*");
    private SqsPanelValidation() {}
    static boolean blank(String value) { return value == null || value.isBlank(); }
    static boolean expression(String value) { return value != null && EXPRESSION.matcher(value).find(); }
    static boolean integer(String value, long min, long max, boolean optional) {
        if (blank(value)) return optional;
        if (expression(value)) return true;
        try { long n = Long.parseLong(value.trim()); return n >= min && n <= max; }
        catch (NumberFormatException e) { return false; }
    }
    static boolean attributes(List<SqsMessageAttribute> rows) {
        if (rows.size() > 10) return false;
        Set<String> names = new HashSet<>();
        for (SqsMessageAttribute row : rows) {
            if (row == null || blank(row.getName()) || row.getValue() == null || row.getValue().isEmpty()) return false;
            String name = row.getName();
            if (!expression(name) && (!name.matches("[A-Za-z0-9_.-]{1,256}") || name.startsWith(".") || name.endsWith(".") || name.contains("..")
                    || name.matches("(?i)^(aws|amazon)\\..*") || !names.add(name))) return false;
            String type = row.getDataType();
            if (type == null || !List.of("String", "Number", "Binary").contains(type)) return false;
            if (!expression(row.getValue())) {
                if ("Number".equals(type) && !row.getValue().matches("[+-]?(?:\\d+(?:\\.\\d*)?|\\.\\d+)(?:[eE][+-]?\\d+)?")) return false;
                if ("Binary".equals(type)) {
                    try { Base64.getDecoder().decode(row.getValue()); } catch (IllegalArgumentException e) { return false; }
                }
            }
        }
        return true;
    }
}
