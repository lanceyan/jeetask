/*
 * @project: jeetask 
 * @package: com.jeeframework.jeetask.zookeeper.worker
 * @title:   Worker.java 
 *
 * Copyright (c) 2017 jeeframework Limited, Inc.
 * All rights reserved.
 */
package com.jeeframework.jeetask.zookeeper.worker;

import com.dangdang.ddframe.job.reg.base.CoordinatorRegistryCenter;
import com.jeeframework.jeetask.event.JobEventBus;
import com.jeeframework.jeetask.event.type.JobExecutionEvent;
import com.jeeframework.jeetask.event.type.JobStatusTraceEvent;
import com.jeeframework.jeetask.task.Job;
import com.jeeframework.jeetask.task.Task;
import com.jeeframework.jeetask.task.context.JobContext;
import com.jeeframework.jeetask.util.net.IPUtils;
import com.jeeframework.jeetask.zookeeper.server.ServerNode;
import com.jeeframework.jeetask.zookeeper.server.ServerService;
import com.jeeframework.jeetask.zookeeper.storage.NodePath;
import com.jeeframework.jeetask.zookeeper.storage.NodeStorage;
import com.jeeframework.jeetask.zookeeper.storage.TransactionExecutionCallback;
import com.jeeframework.util.classes.ClassUtils;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.transaction.CuratorTransactionFinal;
import org.apache.curator.framework.recipes.atomic.DistributedAtomicInteger;
import org.apache.curator.retry.RetryNTimes;
import org.springframework.beans.factory.BeanFactory;

import java.util.Date;

/**
 * job执行者
 *
 * @author lance
 * @version 1.0 2017-08-24 17:59
 */
@Slf4j
public class Worker implements Runnable {
    private final Task task;
    private final NodeStorage nodeStorage;
    private final ServerNode serverNode;
    private final NodePath nodePath;
    private final JobEventBus jobEventBus;
    private final ServerService serverService;
    private final CoordinatorRegistryCenter regCenter;
    private final BeanFactory context;

    public Worker(final CoordinatorRegistryCenter regCenter, final JobEventBus
            jobEventBus, final Task task, final BeanFactory context, final ServerService serverService) {
        this.regCenter = regCenter;
        nodeStorage = new NodeStorage(regCenter);
        this.jobEventBus = jobEventBus;
        serverNode = new ServerNode();
        nodePath = new NodePath();
        this.serverService = serverService;
        this.task = task;
        this.context = context;
    }

    @Override
    public void run() {
        try {
            doJob();
        } catch (Throwable e) {

        } finally {

        }
    }

    public void doJob() {
        String jobClass = task.getJobClass();
        long taskId = task.getId();
        String ip = IPUtils.getUniqueServerId();
        try {
            JobExecutionEvent jobExecutionEvent = new JobExecutionEvent(taskId, JobStatusTraceEvent.State
                    .TASK_RUNNING, ip);
            jobExecutionEvent.setStartTime(new Date());
            jobEventBus.trigger(jobExecutionEvent);

            Class jobClazz = ClassUtils.forName(jobClass);
            Job job = (Job) jobClazz.newInstance();

            JobContext jobContext = new JobContext();
            jobContext.setContext(context);
            jobContext.setTask(task);
            job.doJob(jobContext);
            nodeStorage.executeInTransaction(new FinishTaskTransactionExecutionCallback(task));
        } catch (Throwable e) {
            nodeStorage.executeInTransaction(new TaskErrorTransactionExecutionCallback(task, e));
        }
    }


    @RequiredArgsConstructor
    class FinishTaskTransactionExecutionCallback implements TransactionExecutionCallback {

        final Task task;

        String outAndLocalIp = IPUtils.getUniqueServerId();

        @Override
        public void execute(final CuratorTransactionFinal curatorTransactionFinal) throws Exception {
            /**   delete  /servers/169.254.79.228_10.0.75.1/tasks/running/111    里删除一个任务
             *    数据库更新任务为完成
             */

            long taskId = task.getId();
            //running任务队列删除
            curatorTransactionFinal.delete().forPath(nodePath.getFullPath(ServerNode.getRunningTaskIdNode
                    (outAndLocalIp, taskId
                    ))).and();


        }

        @Override
        public void afterCommit() throws Exception {
            long taskId = task.getId();
            JobExecutionEvent jobExecutionEvent = new JobExecutionEvent(taskId, JobStatusTraceEvent.State
                    .TASK_FINISHED, outAndLocalIp);
            jobExecutionEvent = jobExecutionEvent.executionSuccess();
            jobEventBus.trigger(jobExecutionEvent);

            //服务器上任务计数  -1
            CuratorFramework client = nodeStorage.getClient();
            DistributedAtomicInteger counter = new DistributedAtomicInteger(client, nodePath.getFullPath(ServerNode
                    .getTaskCountNode
                            (outAndLocalIp)), new
                    RetryNTimes(100, 1000));
            counter.decrement();

            int taskCount = counter.get().postValue();
            log.debug("taskId =  " + taskId + "  执行完成了！  taskCount =  " + taskCount + "  条任务。");
        }
    }


    @RequiredArgsConstructor
    class TaskErrorTransactionExecutionCallback implements TransactionExecutionCallback {

        final Task task;
        final Throwable throwable;



        String outAndLocalIp = IPUtils.getUniqueServerId();

        @Override
        public void execute(final CuratorTransactionFinal curatorTransactionFinal) throws Exception {
            long taskId = task.getId();
            /**   delete  /servers/169.254.79.228_10.0.75.1/tasks/running/111    里删除一个任务
             *    数据库更新错误信息为出错
             */
            //running任务队列删除
            curatorTransactionFinal.delete().forPath(nodePath.getFullPath(ServerNode.getRunningTaskIdNode
                    (outAndLocalIp, taskId
                    ))).and();
        }

        @Override
        public void afterCommit() throws Exception {
            long taskId = task.getId();
            String ip = IPUtils.getUniqueServerId();

            JobExecutionEvent jobExecutionEvent = new JobExecutionEvent(taskId, JobStatusTraceEvent.State
                    .TASK_ERROR, ip);
            jobExecutionEvent = jobExecutionEvent.executionFailure(throwable);

            jobEventBus.trigger(jobExecutionEvent);

            //服务器上任务计数  -1
            CuratorFramework client = nodeStorage.getClient();
            DistributedAtomicInteger counter = new DistributedAtomicInteger(client, nodePath.getFullPath(ServerNode
                    .getTaskCountNode
                            (outAndLocalIp)), new
                    RetryNTimes(100, 1000));
            counter.decrement();

            int taskCount = counter.get().postValue();
            log.debug("taskId =  " + taskId + "  执行出错啦！ taskCount =  " + taskCount + "  条任务。");
        }
    }
}
