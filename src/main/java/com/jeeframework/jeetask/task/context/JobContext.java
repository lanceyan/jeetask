/*
 * @project: jeetask 
 * @package: com.jeeframework.jeetask.task.context
 * @title:   JobContext.java 
 *
 * Copyright (c) 2017 jeeframework Limited, Inc.
 * All rights reserved.
 */
package com.jeeframework.jeetask.task.context;

import com.jeeframework.jeetask.task.Task;
import lombok.Getter;
import lombok.Setter;
import org.springframework.beans.factory.BeanFactory;

/**
 * 执行作业的上下文
 *
 * @author lance
 * @version 1.0 2017-08-30 15:58
 */
public class JobContext {
    @Setter
    @Getter
    private BeanFactory context;
    @Setter
    @Getter
    private Task task;
}
