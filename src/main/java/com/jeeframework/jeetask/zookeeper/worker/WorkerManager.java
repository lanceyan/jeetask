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
import org.springframework.beans.factory.BeanFactory;

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
    private final BeanFactory context;

    private final CoordinatorRegistryCenter regCenter;

    public WorkerManager(final CoordinatorRegistryCenter regCenter, final JobEventBus
            jobEventBus, final BeanFactory context, final int workerNum, final ServerService serverService) {
        this.regCenter = regCenter;
        nodeStorage = new NodeStorage(regCenter);
        this.jobEventBus = jobEventBus;
        serverNode = new ServerNode();
        nodePath = new NodePath();
        this.serverService = serverService;
        taskService = new TaskService(regCenter, jobEventBus);
        this.context = context;

        int currentWorkerNum = workerNum > 0 ? workerNum : Runtime.getRuntime().availableProcessors() * 2;
        System.out.println("===============根据配置系统启动了：  " + currentWorkerNum + "个worker  ==================");

        ExecutorServiceObject executorServiceObject = new ExecutorServiceObject("worker-executor", currentWorkerNum);
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
                    workerExecutorService.execute(new Worker(regCenter, jobEventBus, taskTmp, context, this
                            .serverService));
                }
            }

            while (true) {
                String batchNo = DateFormatUtils.format(new Date(), "yyyyMMddHHmmss");
                int successCount = 0;
                int allCount = 0;
                try {
                    List<String> waitingTaskIds = serverService.getWaitingTaskIds();
                    allCount = (Validate.isEmpty(waitingTaskIds) ? 0 :
                            waitingTaskIds.size());
                    log.info("当前批次： " + batchNo + " 取出来任务数：" + allCount);
                    if (!Validate.isEmpty(waitingTaskIds)) {
                        for (String taskIdTmp : waitingTaskIds) {
                            long taskId = Long.valueOf(taskIdTmp);
                            Task taskTmp = serverService.getWaitingTaskById(taskId);
                            //执行分配任务，认领任务
                            serverService.claimTask(taskTmp, workerExecutorService);
                            successCount++;

                        }
                    }

                } catch (Throwable e) {
                    log.error("当前批次： " + batchNo + " 执行出错 " + e, e);
                } finally {
                    try {
                        Thread.sleep(5000);
                        log.info("当前批次： " + batchNo + " 执行完成，allCount = " + allCount + " ，successCount =  " +
                                successCount + "  休息5秒");
                    } catch (InterruptedException e) {
                    }
                }
            }
        } finally {
            workerExecutorService.shutdownNow();
        }

    }
}
