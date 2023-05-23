package com.pan.flink.utils;

import java.io.File;
import java.io.IOException;
import java.net.JarURLConnection;
import java.net.URL;
import java.util.*;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

/**
 * @author panjb
 */
public class ClasspathPackageScanner {

    public static final String PROTOCOL_JAR = "jar";
    public static final String PROTOCOL_FILE = "file";
    public static final String CLASS_FILE = ".class";

    private ClasspathPackageScanner() {
    }

    public static List<Class<?>> scan(String basePackage) {
        return doScan(basePackage, Thread.currentThread().getContextClassLoader());
    }

    public static List<Class<?>> scan(String basePackage, ClassLoader classLoader) {
        return doScan(basePackage, classLoader);
    }

    private static List<Class<?>> doScan(String basePackage, ClassLoader classLoader) {
        String path = basePackage.replaceAll("\\.", "/");
        URL url = classLoader.getResource(path);
        if (url == null) {
            return Collections.emptyList();
        }
        List<Class<?>> allClass = new ArrayList<>();
        String protocol = url.getProtocol();
        if (PROTOCOL_JAR.equalsIgnoreCase(protocol)) {
            allClass.addAll(getJarClasses(url, basePackage));
        } else if (PROTOCOL_FILE.equalsIgnoreCase(protocol)) {
            allClass.addAll(getFileClasses(url, basePackage));
        }
        return allClass;
    }

    private static List<Class<?>> getJarClasses(URL url, String packagePath) {
        List<Class<?>> res = new ArrayList<>();
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

    public static List<Class<?>> getFileClasses(URL url, String packagePath) {
        packagePath = packagePath.replaceAll("\\.", "/");
        List<Class<?>> allClasses = new ArrayList<>();
        String filePath = url.getFile();
        File dir = new File(filePath);
        List<File> classFiles = new ArrayList<>();
        listFiles(dir, classFiles);
        try {
            for (File file : classFiles) {
                String fPath = file.getAbsolutePath().replaceAll("\\\\","/") ;
                String className = fPath.substring(fPath.lastIndexOf(packagePath));
                className = className.replace(".class","").replaceAll("/", ".");
                Class<?> aClass = Class.forName(className);
                allClasses.add(aClass);
            }
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
        return allClasses;
    }

    private static void listFiles(File dir, List<File> fileList) {
        if (dir.isDirectory()) {
            File[] files = dir.listFiles();
            if (files != null) {
                for (File file : files) {
                    listFiles(file, fileList);
                }
            }
        } else {
            if(dir.getName().endsWith(CLASS_FILE)) {
                fileList.add(dir);
            }
        }
    }
}
