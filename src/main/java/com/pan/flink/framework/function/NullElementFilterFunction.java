package com.pan.flink.framework.function;

import org.apache.flink.api.common.functions.FilterFunction;

/**
 * null值过滤器
 *
 * @author panjb
 */
public class NullElementFilterFunction<T> implements FilterFunction<T> {
    @Override
    public boolean filter(T value) throws Exception {
        return value != null;
    }
}
