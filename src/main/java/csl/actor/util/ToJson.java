package csl.actor.util;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;

public interface ToJson {
    Map<String,Object> toJson(Function<Object,Object> valueConverter);

    default Object toJson(Function<Object,Object> valueConverter, Object o) {
        return valueConverter.apply(o);
    }

    default Object toJson(Function<Object,Object> valueConverter, Object o, Object nullVal) {
        return o == null ? valueConverter.apply(nullVal) : valueConverter.apply(o);
    }

    static void write(Consumer<String> out, Object json) {
        if (json == null) {
            out.accept("null");
        } else if (json instanceof List<?>) {
            out.accept("[");
            boolean first = true;
            for (Object o : (List<?>) json) {
                if (first) {
                    first = false;
                } else {
                    out.accept(",");
                }
                write(out, o);
            }
            out.accept("]");
        } else if (json instanceof Map<?,?>) {
            out.accept("{");
            boolean first = true;
            for (Map.Entry<?,?> e : ((Map<?,?>) json).entrySet()) {
                if (first) {
                    first = false;
                } else {
                    out.accept(",");
                }
                write(out, e.getKey());
                out.accept(":");
                write(out, e.getValue());
            }
            out.accept("}");
        } else if (json instanceof String) {
            out.accept(ToJson.toSourceString((String) json));
        } else if (json instanceof Number) {
            out.accept(Objects.toString(json));
        } else if (json instanceof Boolean) {
            out.accept(Objects.toString(json));
        } else {
            out.accept(ToJson.toSourceString(Objects.toString(json)));
        }
    }

    static String toSourceString(String data) {
        StringBuilder buf = new StringBuilder();
        buf.append("\"");
        for (char c : data.toCharArray()) {
            if (c == '\n') {
                buf.append("\\").append("n");
            } else if (c == '\t') {
                buf.append("\\").append("t");
            } else if (c == '\f') {
                buf.append("\\").append("f");
            } else if (c == '\r') {
                buf.append("\\").append("r");
            } else if (c == '\b') {
                buf.append("\\").append("b");
            } else if (c == '\"') {
                buf.append("\\").append("\"");
            } else if (c == '\\') {
                buf.append("\\").append("\\");
            } else if (Character.isISOControl(c)) {
                buf.append(String.format("\\u%04x", (int) c));
            } else {
                buf.append(c);
            }
        }
        buf.append("\"");
        return buf.toString();
    }


}
