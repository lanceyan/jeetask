/*
 * @project: jeetask 
 * @package: com.jeeframework.jeetask.zookeeper.exception
 * @title:   ZkOperationException.java 
 *
 * Copyright (c) 2017 jeeframework Limited, Inc.
 * All rights reserved.
 */
package com.jeeframework.jeetask.zookeeper.exception;

/**
 * zk操作异常
 *
 * @author lance
 * @version 1.0 2017-09-29 11:07
 */
public class ZkOperationException extends RuntimeException {
    public ZkOperationException(String msg) {
        super(msg);
    }

    public ZkOperationException(final Exception cause) {
        super(cause);
    }

}
