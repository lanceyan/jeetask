/*
 * @project: jeetask 
 * @package: com.jeeframework.jeetask.executor
 * @title:   ExecutorShutdownException.java 
 *
 * Copyright (c) 2017 jeeframework Limited, Inc.
 * All rights reserved.
 */
package com.jeeframework.jeetask.executor;

/**
 * excutor关闭异常
 *
 * @author lance
 * @version 1.0 2017-08-24 17:52
 */
public class ExecutorShutdownException extends RuntimeException {
    public ExecutorShutdownException(String message) {
        super(message);
    }
}
