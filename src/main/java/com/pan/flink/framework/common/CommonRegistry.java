package com.pan.flink.framework.common;


import com.pan.flink.framework.Registry;
import com.pan.flink.framework.annotation.Component;
import com.pan.flink.utils.ClasspathPackageScanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.Serializable;
import java.lang.reflect.ParameterizedType;
import java.util.*;

/**
 * Common component registry
 *
 * <p>Register the component according to the annotation {@link Component} and the specified type
 *
 * @param <T> Type of the component
 * @author panjb
 */
public class CommonRegistry<T> implements Serializable, Registry<T> {
    private static final long serialVersionUID = 3452369488810257598L;

    private final static Logger logger = LoggerFactory.getLogger(CommonRegistry.class);
    private final Map<String, T> components = new HashMap<>();

    public CommonRegistry(String packageName) {
        ParameterizedType type = (ParameterizedType) this.getClass().getGenericSuperclass();
        Class<T> tClass = (Class<T>) type.getActualTypeArguments()[0];
        this.scanAndRegister(packageName, tClass);
    }

    public CommonRegistry(String packageName, Class<T> tClass) {
        this.scanAndRegister(packageName, tClass);
    }

    @Override
    public final T get(String key) {
        return this.components.get(key);
    }

    @Override
    public List<T> getAll() {
        return new ArrayList<>(this.components.values());
    }

    @Override
    public final void register(Class<T> tClass) {
        try {
            Component component = tClass.getAnnotation(Component.class);
            if (component == null) {
                if (logger.isErrorEnabled()) {
                    logger.error("The component [{}] must be configured with annotation [{}}]", tClass, Component.class.getName());
                }
                return;
            }
            T t = tClass.newInstance();
            String[] names = component.names();
            if (names.length > 0) {
                Arrays.stream(names).forEach(name -> this.components.put(name, t));
            } else {
                this.components.put(component.value(), t);
            }
            if (logger.isDebugEnabled()) {
                logger.debug("Register component: [{}]", component.value());
            }
        } catch (InstantiationException | IllegalAccessException e) {
            if (logger.isErrorEnabled()) {
                logger.error("Create an instance of component class [{}] failure!", tClass);
            }
        }
    }

    private void scanAndRegister(String pkgName, Class<T> tClass) {
        if (logger.isDebugEnabled()) {
            logger.debug("Scanning package [{}].", pkgName);
        }
        Set<Class<?>> allClass = ClasspathPackageScanner.scan(pkgName);
        for (Class<?> aClass : allClass) {
            if (tClass.isAssignableFrom(aClass) && aClass.getAnnotation(Component.class) != null) {
                this.register((Class<T>) aClass);
            }
        }
    }
}
