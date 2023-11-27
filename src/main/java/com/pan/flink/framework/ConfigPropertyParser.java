package com.pan.flink.framework;

import com.pan.flink.framework.annotation.ConfigProperty;
import org.apache.flink.api.java.utils.ParameterTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author panjb
 */
public class ConfigPropertyParser {

    private final static Logger logger = LoggerFactory.getLogger(ConfigPropertyParser.class);

    private ConfigPropertyParser() {
    }

    public static <T> void parseConfig(T config, ParameterTool parameterTool) {
        parseConfig(config, Object.class, parameterTool);
    }

    /**
     * Parse and populate the properties of current bean configuration annotated {@link ConfigProperty}
     * from {@link ParameterTool}
     *
     * @param config Objects populated with configuration properties
     * @param stopClass super class
     * @param parameterTool config
     */
    public static <T> void parseConfig(T config, Class<?> stopClass, ParameterTool parameterTool) {
        Field[] fields = getAllFields(config.getClass(), stopClass);
        try {
            for (Field field : fields) {
                ConfigProperty property = field.getAnnotation(ConfigProperty.class);
                if (property != null) {
                    Object value = getConfigValue(parameterTool, field.getType(), property.value());
                    if (logger.isDebugEnabled()) {
                        logger.debug("param: {} = {}", property.value(), value);
                    }
                    if (PropertyValueConverter.DefConverter.class != property.converter()) {
                        value = property.converter().newInstance().apply(value);
                    }
                    field.setAccessible(true);
                    field.set(config, value);
                }
            }
        } catch (IllegalAccessException | InstantiationException e) {
            if (logger.isErrorEnabled()) {
                logger.error("parse config property error", e);
            }
            throw new RuntimeException(e);
        }
    }

    public static Field[] getAllFields(Class<?> clazz, Class<?> stopClass) {
        List<Field> fieldList = new ArrayList<>();
        while (clazz != null && clazz != stopClass){
            fieldList.addAll(Arrays.asList(clazz.getDeclaredFields()));
            clazz = clazz.getSuperclass();
        }
        Field[] fields = new Field[fieldList.size()];
        return fieldList.toArray(fields);
    }

    private static Object getConfigValue(ParameterTool parameterTool, Class<?> type, String key) {
        if (int.class == type || Integer.class == type) {
            return parameterTool.getInt(key);
        }
        if (long.class == type || Long.class == type) {
            return parameterTool.getLong(key);
        }
        if (double.class == type || Double.class == type) {
            return parameterTool.getDouble(key);
        }
        if (boolean.class == type || Boolean.class == type) {
            return parameterTool.getBoolean(key, false);
        }
        if (float.class == type || Float.class == type) {
            return parameterTool.getFloat(key);
        }
        if (short.class == type || Short.class == type) {
            return parameterTool.getShort(key);
        }
        return parameterTool.get(key);
    }
}
