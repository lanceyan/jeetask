/*
 * @project: jeetask 
 * @package: com.jeeframework.jeetask.zookeeper.worker
 * @title:   WorkerManager.java 
 *
 * Copyright (c) 2017 jeeframework Limited, Inc.
 * All rights reserved.
 */
package com.jeeframework.jeetask.zookeeper.worker;

import com.dangdang.ddframe.job.reg.base.CoordinatorRegistryCenter;
import com.dangdang.ddframe.job.util.concurrent.ExecutorServiceObject;
import com.jeeframework.jeetask.event.JobEventBus;
import com.jeeframework.jeetask.task.Task;
import com.jeeframework.jeetask.zookeeper.server.ServerNode;
import com.jeeframework.jeetask.zookeeper.server.ServerService;
import com.jeeframework.jeetask.zookeeper.storage.NodePath;
import com.jeeframework.jeetask.zookeeper.storage.NodeStorage;
import com.jeeframework.jeetask.zookeeper.task.TaskService;
import com.jeeframework.util.validate.Validate;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.time.DateFormatUtils;

import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutorService;

/**
 * 工作线程管理者
 *
 * @author lance
 * @version 1.0 2017-08-24 17:58
 */
@Slf4j
public class WorkerManager implements Runnable {
    private final NodeStorage nodeStorage;
    private final ServerNode serverNode;
    private final NodePath nodePath;
    private final JobEventBus jobEventBus;
    private final ExecutorService workerExecutorService;
    private final ServerService serverService;
    private final TaskService taskService;

    private final CoordinatorRegistryCenter regCenter;

    public WorkerManager(final CoordinatorRegistryCenter regCenter, final JobEventBus
            jobEventBus) {
        this.regCenter = regCenter;
        nodeStorage = new NodeStorage(regCenter);
        this.jobEventBus = jobEventBus;
        serverNode = new ServerNode();
        nodePath = new NodePath();
        serverService = new ServerService(regCenter, jobEventBus);
        taskService = new TaskService(regCenter, jobEventBus);

        ExecutorServiceObject executorServiceObject = new ExecutorServiceObject("worker-executor", Runtime.getRuntime()
                .availableProcessors() * 2);
        workerExecutorService = executorServiceObject.createExecutorService();
    }

    @Override
    public void run() {
        try {
            //启动后线检查是否有上次没有完成的任务，取出来继续运行
            List<String> runningTaskIds = serverService.getRunningTaskIds();
            if (!Validate.isEmpty(runningTaskIds)) {
                for (String taskIdTmp : runningTaskIds) {
                    long taskId = Long.valueOf(taskIdTmp);
                    Task taskTmp = serverService.getRunningTaskById(taskId);
                    //执行分配任务，认领任务
                    workerExecutorService.execute(new Worker(regCenter, jobEventBus, taskTmp));
                }
            }

            while (true) {
                String batchNo = DateFormatUtils.format(new Date(), "yyyyMMddHHmmss");
                try {
                    List<String> waitingTaskIds = serverService.getWaitingTaskIds();
                    if (!Validate.isEmpty(waitingTaskIds)) {
                        for (String taskIdTmp : waitingTaskIds) {
                            long taskId = Long.valueOf(taskIdTmp);
                            Task taskTmp = serverService.getWaitingTaskById(taskId);
                            //执行分配任务，认领任务
                            serverService.claimTask(taskTmp, workerExecutorService);

                        }
                    }

                } catch (Throwable e) {

                } finally {
                    try {
                        Thread.sleep(5000);
                        log.info("当前批次： " + batchNo + " 执行完成，休息5秒");
                    } catch (InterruptedException e) {
                    }
                }
            }
        } finally {
            workerExecutorService.shutdownNow();
        }

    }
}
