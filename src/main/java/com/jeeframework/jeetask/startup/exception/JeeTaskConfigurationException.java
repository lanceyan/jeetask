/*
 * @project: jeetask 
 * @package: com.jeeframework.jeetask.startup.exception
 * @title:   NoEmbedPortConfigurationException.java 
 *
 * Copyright (c) 2017 jeeframework Limited, Inc.
 * All rights reserved.
 */
package com.jeeframework.jeetask.startup.exception;

/**
 * jeetask配置错误异常
 *
 * @author lance
 * @version 1.0 2017-08-30 12:54
 */
public class JeeTaskConfigurationException extends RuntimeException {
    public JeeTaskConfigurationException(String s) {
        super(s);
    }
}
