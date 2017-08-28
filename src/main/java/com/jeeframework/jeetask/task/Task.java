/*
 * @project: jeetask 
 * @package: com.jeeframework.jeetask.task
 * @title:   Task.java 
 *
 * Copyright (c) 2017 jeeframework Limited, Inc.
 * All rights reserved.
 */
package com.jeeframework.jeetask.task;

import lombok.Getter;
import lombok.Setter;

/**
 * 任务定义类
 *
 * @author lance
 * @version 1.0 2017-07-28 18:55
 */
@Getter
@Setter
public class Task {

    private long id;
    private String name;

    private String jobClass;
    private String param;


}
