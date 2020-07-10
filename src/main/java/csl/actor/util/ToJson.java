package csl.actor.util;

import java.util.Map;
import java.util.function.Function;

public interface ToJson {
    Map<String,Object> toJson(Function<Object,Object> valueConverter);

    default Object toJson(Function<Object,Object> valueConverter, Object o) {
        return valueConverter.apply(o);
    }

    default Object toJson(Function<Object,Object> valueConverter, Object o, Object nullVal) {
        return o == null ? valueConverter.apply(nullVal) : valueConverter.apply(o);
    }
}
