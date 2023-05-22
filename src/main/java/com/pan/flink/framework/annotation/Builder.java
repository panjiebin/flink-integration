package com.pan.flink.framework.annotation;

import java.lang.annotation.*;

/**
 * @author panjb
 */
@Documented
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface Builder {

    Class<?> value();
}
