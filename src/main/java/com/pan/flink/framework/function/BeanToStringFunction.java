package com.pan.flink.framework.function;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonPropertyOrder;
import org.apache.flink.api.common.functions.MapFunction;

import java.beans.PropertyDescriptor;
import java.util.StringJoiner;

/**
 * @author panjb
 */
public class BeanToStringFunction<T> implements MapFunction<T, String> {

    private final String delimiter;

    private final static String DEF_DELIMITER = ",";

    public BeanToStringFunction() {
        this(DEF_DELIMITER);
    }

    public BeanToStringFunction(String delimiter) {
        this.delimiter = delimiter;
    }

    @Override
    public String map(T thresholds) throws Exception {
        Class<?> aClass = thresholds.getClass();
        StringJoiner sj = new StringJoiner(delimiter);
        JsonPropertyOrder annotation = aClass.getDeclaredAnnotation(JsonPropertyOrder.class);
        if (annotation == null) {
            return null;
        }
        String[] fields = annotation.value();
        for (String fieldName : fields) {
            PropertyDescriptor descriptor = new PropertyDescriptor(fieldName, aClass);
            Object value = descriptor.getReadMethod().invoke(thresholds);
            sj.add(value + "");
        }
        return sj.toString();
    }
}
