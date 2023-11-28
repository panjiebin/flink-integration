package com.pan.flink.utils;

import org.apache.flink.api.java.utils.ParameterTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;

/**
 * @author panjb
 */
public class ClasspathConfigFileLoader {

    private final static Logger logger = LoggerFactory.getLogger(ClasspathConfigFileLoader.class);

    private ClasspathConfigFileLoader() {
    }

    public static ParameterTool load(String fileName) throws IOException {
        InputStream is = ClasspathConfigFileLoader.class.getClassLoader().getResourceAsStream(fileName);
        if (is == null) {
            if (logger.isWarnEnabled()) {
                logger.warn("Could not find configuration file [{}] in the classpath!", fileName);
            }
            return null;
        }
        return ParameterTool.fromPropertiesFile(is);
    }
}
