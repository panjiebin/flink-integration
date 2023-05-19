package com.pan.flink.utils;

import java.io.File;
import java.io.IOException;
import java.net.JarURLConnection;
import java.net.URL;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Set;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

/**
 * @author panjb
 */
public class ClasspathPackageScanner {

    public static final String PROTOCOL_JAR = "jar";
    public static final String PROTOCOL_FILE = "file";

    public static Set<Class<?>> scan(String basePackage) {
        return doScan(basePackage, Thread.currentThread().getContextClassLoader());
    }

    public static Set<Class<?>> scan(String basePackage, ClassLoader classLoader) {
        return doScan(basePackage, classLoader);
    }

    private static Set<Class<?>> doScan(String basePackage, ClassLoader classLoader) {
        String path = basePackage.replaceAll("\\.", "/");
        URL url = classLoader.getResource(path);
        if (url == null) {
            return Collections.emptySet();
        }
        Set<Class<?>> allClass = new HashSet<>();
        String protocol = url.getProtocol();
        if (PROTOCOL_JAR.equalsIgnoreCase(protocol)) {
            allClass.addAll(getJarClasses(url, basePackage));
        } else if (PROTOCOL_FILE.equalsIgnoreCase(protocol)) {
            allClass.addAll(getFileClasses(url, basePackage));
        }
        return allClass;
    }

    private static Set<Class<?>> getJarClasses(URL url, String packagePath) {
        Set<Class<?>> res = new HashSet<>();
        try {
            JarURLConnection conn = (JarURLConnection) url.openConnection();
            if (conn != null) {
                JarFile jarFile = conn.getJarFile();
                Enumeration<JarEntry> entries = jarFile.entries();
                while (entries.hasMoreElements()) {
                    JarEntry jarEntry = entries.nextElement();
                    String name = jarEntry.getName();
                    if (name.contains(".class") && name.replaceAll("/", ".").startsWith(packagePath)) {
                        String className = name.substring(0, name.lastIndexOf(".")).replace("/", ".");
                        try {
                            Class<?> clazz = Class.forName(className);
                            res.add(clazz);
                        } catch (ClassNotFoundException e) {
                            throw new RuntimeException(e);
                        }
                    }
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return res;
    }

    public static Set<Class<?>> getFileClasses(URL url, String packagePath) {
        Set<Class<?>> allClasses = new HashSet<>();
        String filePath = url.getFile();
        File dir = new File(filePath);
        String[] fileNames = dir.list();
        if (fileNames == null) {
            return allClasses;
        }
        for (String fileName : fileNames) {
            if (fileName.endsWith(".class")) {
                fileName = fileName.substring(0, fileName.indexOf(".class"));
                try {
                    Class<?> aClass = Class.forName(packagePath + "." + fileName);
                    allClasses.add(aClass);
                } catch (ClassNotFoundException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        return allClasses;
    }
}
