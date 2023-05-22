package com.pan.flink.framework.annotation;

import java.lang.annotation.*;

/**
 * Annotation identifying the component
 *
 * @author panjb
 */
@Documented
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface Component {

    /**
     * @return Key of component
     */
    String value() default "default";

    /**
     * @return Keys of component
     */
    String[] names() default {};
}
