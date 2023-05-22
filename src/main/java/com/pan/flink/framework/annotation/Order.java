package com.pan.flink.framework.annotation;

import java.lang.annotation.*;

/**
 * @author panjb
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface Order {
    int value();
}
