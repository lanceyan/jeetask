/*
 * @project: jeetask 
 * @package: com.jeeframework.jeetask.task
 * @title:   Task.java 
 *
 * Copyright (c) 2017 jeeframework Limited, Inc.
 * All rights reserved.
 */
package com.jeeframework.jeetask.task;

import com.google.gson.Gson;
import com.jeeframework.util.classes.ClassUtils;
import com.jeeframework.util.string.apache.StringUtils;
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

    protected long id;
    protected String name;

    protected String jobClass;
    protected String param;

    private static final String ZK_TASK_SEPERATE = ";";

    public static <T> T fromZk(String zkTask) throws ClassNotFoundException {
        String className = StringUtils.substringBefore(zkTask, ZK_TASK_SEPERATE);
        String taskJSON = StringUtils.substringAfter(zkTask, ZK_TASK_SEPERATE);

        Class<T> taskClass = ClassUtils.forName(className);

        Gson gson = new Gson();
        T task = gson.fromJson(taskJSON, taskClass);

        return task;
    }

    public String toZk() {
        Gson gson = new Gson();
        String taskJSON = gson.toJson(this);

        String taskName = this.getClass().getName();

        return taskName + ";" + taskJSON;
    }

}
