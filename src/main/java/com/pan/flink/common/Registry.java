package com.pan.flink.common;

import java.util.List;

/**
 * The base interface for component registry
 *
 * @param <T> Type of the component
 * @author panjb
 */
public interface Registry<T> {

    /**
     * Register a component
     * @param tClass Class of the special component
     */
    void register(Class<T> tClass);

    /**
     * Get a component by special key
     * @param key Component key
     * @return Key corresponding component
     */
    T get(String key);

    /**
     * Get a component by special key.
     * If the component corresponding to key does not exist,
     * the component corresponding to key=default is returned
     * @param key Component key
     * @return Key corresponding component
     */
    default T getOrDefault(String key) {
        T t = get(key);
        if (t == null) {
            return get("default");
        }
        return t;
    }

    /**
     * get all components
     * @return components
     */
    List<T> getAll();
}
