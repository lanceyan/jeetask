/*
 * @project: jeetask 
 * @package: com.jeeframework.jeetask.util.resource
 * @title:   JeeTaskPropertiesUtil.java 
 *
 * Copyright (c) 2017 jeeframework Limited, Inc.
 * All rights reserved.
 */
package com.jeeframework.jeetask.util.resource;

import com.jeeframework.util.classes.ClassUtils;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;
import java.util.Properties;

/**
 * jeetask.properties的工具类
 *
 * @author lance
 * @version 1.0 2017-08-25 18:29
 */
public class JeeTaskPropertiesUtil {
    //任务调度系统的配置文件
    private static final String TASK_CONFIG_FILE = "task.properties";

    public static Properties getProperties() throws IOException {
        Properties properties = new Properties();
        ClassLoader clToUse = ClassUtils.getDefaultClassLoader();
        URL url = clToUse.getResource(TASK_CONFIG_FILE);
        if (url == null) {
            throw new FileNotFoundException(TASK_CONFIG_FILE + " file not found  ");
        }
        InputStream is = null;
        try {
            URLConnection con = url.openConnection();
            con.setUseCaches(false);
            is = con.getInputStream();
            properties.load(is);
        } finally {
            if (is != null) {
                is.close();
            }
        }
        return properties;
    }
}
