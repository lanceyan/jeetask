/*
 * @project: jeetask 
 * @package: com.jeeframework.jeetask.zookeeper.exception
 * @title:   ZkOperationException.java 
 *
 * Copyright (c) 2017 jeeframework Limited, Inc.
 * All rights reserved.
 */
package com.jeeframework.jeetask.event.exception;

/**
 * 任务事件操作异常
 *
 * @author lance
 * @version 1.0 2017-09-29 11:07
 */
public class JobEventException extends RuntimeException {
    public JobEventException(String msg) {
        super(msg);
    }

    public JobEventException(final Exception cause) {
        super(cause);
    }

    public JobEventException(final String msg, final Exception cause) {
        super(msg, cause);
    }

}
