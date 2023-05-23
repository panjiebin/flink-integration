package com.pan.flink.framework.annotation;


import com.pan.flink.framework.PropertyValueConverter;

import java.lang.annotation.*;

/**
 * Annotation identifying the config property
 *
 * @author panjb
 */
@Documented
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface ConfigProperty {

    /**
     * @return Key of config
     */
    String value();

    Class<? extends PropertyValueConverter> converter() default PropertyValueConverter.DefConverter.class;
}
