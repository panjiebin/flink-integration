package com.pan.flink.framework;

import java.io.Serializable;
import java.util.function.Function;

/**
 * @author panjb
 */
public interface PropertyValueConverter<T> extends Function<String, T>, Serializable {

    class DefConverter implements PropertyValueConverter<String> {
        private static final long serialVersionUID = 7994157047786853790L;
        @Override
        public String apply(String s) {
            return s;
        }
    }
}
