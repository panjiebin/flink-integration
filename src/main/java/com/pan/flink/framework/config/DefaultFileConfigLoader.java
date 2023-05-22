package com.pan.flink.framework.config;

import com.pan.flink.framework.ConfigLoader;
import com.pan.flink.utils.ClasspathConfigFileLoader;
import org.apache.flink.api.java.utils.ParameterTool;

/**
 * Default configuration file loader
 * <p>
 * Read configuration files in the classpath
 * @author panjb
 */
public class DefaultFileConfigLoader implements ConfigLoader {

    @Override
    public ParameterTool load(String jobName) throws Exception {
        return ClasspathConfigFileLoader.load(jobName + ".properties");
    }
}
