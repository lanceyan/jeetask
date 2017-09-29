/*
 * @project: jeetask 
 * @package: com.jeeframework.jeetask.startup
 * @title:   JeeTaskClient.java 
 *
 * Copyright (c) 2017 jeeframework Limited, Inc.
 * All rights reserved.
 */
package com.jeeframework.jeetask.startup;

import com.jeeframework.jeetask.task.Task;
import com.jeeframework.jeetask.zookeeper.task.TaskService;

import javax.sql.DataSource;
import java.io.IOException;

/**
 * 任务调度系统客户端
 *
 * @author lance
 * @version 1.0 2017-08-17 18:10
 */
public class JeeTaskClient extends JeeTask {


    private TaskService taskService;


    public JeeTaskClient(DataSource dataSource) throws IOException {
        super(dataSource);
    }

    public JeeTaskClient() throws IOException {
        super(null);
    }

    public void start() {
        taskService = new TaskService(regCenter, jobEventBus);
    }

    /**
     * 提交任务
     *
     * @param task
     */
    public void submitTask(Task task) {
        taskService.submitTask(task);
    }

    /**
     * 停止任务，正在运行的任务不能停止
     *
     * @param taskId
     */
    public void stopTask(long taskId) {
        taskService.stopTask(taskId);
    }

    /**
     * 恢复任务，之前出错，停止的任务，恢复在zookeeper里的状态
     *
     * @param task
     */
    public void recoverTask(Task task) {
        taskService.recoverTask(task);
    }


}
