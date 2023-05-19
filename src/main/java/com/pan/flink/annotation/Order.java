package com.pan.flink.annotation;

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
