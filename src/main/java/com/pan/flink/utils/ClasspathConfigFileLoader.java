package com.pan.flink.utils;

import org.apache.flink.api.java.utils.ParameterTool;

import java.io.IOException;
import java.io.InputStream;

/**
 * @author panjb
 */
public class ClasspathConfigFileLoader {

    private ClasspathConfigFileLoader() {
    }

    public static ParameterTool load(String fileName) throws IOException {
        InputStream is = ClasspathConfigFileLoader.class.getClassLoader().getResourceAsStream(fileName);
        if (is == null) {
            throw new RuntimeException("Could not find configuration file [" + fileName + "] in the classpath!");
        }
        return ParameterTool.fromPropertiesFile(is);
    }
}
