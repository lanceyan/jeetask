/*
 * Copyright 1999-2015 dangdang.com.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * </p>
 */

package com.jeeframework.jeetask.zookeeper.task;

import com.dangdang.ddframe.job.reg.base.CoordinatorRegistryCenter;
import com.jeeframework.jeetask.event.JobEventBus;
import com.jeeframework.jeetask.event.type.JobExecutionEvent;
import com.jeeframework.jeetask.event.type.JobStatusTraceEvent;
import com.jeeframework.jeetask.task.Task;
import com.jeeframework.jeetask.util.net.IPUtils;
import com.jeeframework.jeetask.zookeeper.exception.ZkOperationException;
import com.jeeframework.jeetask.zookeeper.exception.ZkOperationExceptionHandler;
import com.jeeframework.jeetask.zookeeper.server.ServerNode;
import com.jeeframework.jeetask.zookeeper.storage.NodePath;
import com.jeeframework.jeetask.zookeeper.storage.NodeStorage;
import com.jeeframework.jeetask.zookeeper.storage.TransactionExecutionCallback;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.transaction.CuratorTransactionFinal;
import org.apache.curator.framework.recipes.atomic.DistributedAtomicInteger;
import org.apache.curator.retry.RetryNTimes;

import java.util.LinkedList;
import java.util.List;

/**
 * 作业服务器服务.
 *
 * @author lance
 */
@Slf4j
public final class TaskService {


    private final NodeStorage nodeStorage;
    private final TaskNode taskNode;
    private JobEventBus jobEventBus;
    private final NodePath nodePath;

    public TaskService(final CoordinatorRegistryCenter regCenter, final JobEventBus jobEventBus) {
        nodeStorage = new NodeStorage(regCenter);
        nodePath = new NodePath();
        taskNode = new TaskNode();
        this.jobEventBus = jobEventBus;
        persistOnline();//创建/tasks节点
    }


    /**
     * 持久化任务信息
     *
     * @param task
     */
    public void submitTask(Task task) {
        JobExecutionEvent jobExecutionEvent = new JobExecutionEvent(task);
//        jobEventBus.post(jobExecutionEvent);
//        JobEventStorage repository = jobEventBus.getJobEventListener().getRepository();

        //实时同步触发事件
        jobEventBus.trigger(jobExecutionEvent);

        task.setId(jobExecutionEvent.getTaskId());

        long taskId = task.getId();

        nodeStorage.fillNode(taskNode.getTaskIdNode(String.valueOf(taskId)), task.toZk());


    }

    /**
     * 停止任务，正在运行的任务不能停止
     *
     * @param taskId
     */
    public void stopTask(long taskId) {
        try {
            String outAndLocalIp = IPUtils.getUniqueServerId();

            nodeStorage.removeNode(TaskNode.getTaskIdNode(String.valueOf(taskId)));
            String ip = IPUtils.getUniqueServerId();

            JobExecutionEvent jobExecutionEvent = new JobExecutionEvent(taskId, JobStatusTraceEvent.State
                    .TASK_STOPPED, ip);
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
            log.debug("taskId =  " + taskId + " 任务停止后， taskCount =  " + taskCount + "  条任务。");
        } catch (Exception ex) {
            try {
                ZkOperationExceptionHandler.handleException(ex);
            } catch (ZkOperationException e) {
                if (null != e.getCause() && e.getCause().toString().contains("NoNodeException")) {
                    throw new ZkOperationException("节点不存在，可能已经被分配执行", e);
                } else {
                    throw e;
                }

            }
        }
    }

    /**
     * 恢复任务，之前出错，停止的任务，恢复在zookeeper里的状态
     *
     * @param task
     */
    public void recoverTask(Task task) {
        try {
            String outAndLocalIp = IPUtils.getUniqueServerId();

            String ip = IPUtils.getUniqueServerId();
            long taskId = task.getId();

            nodeStorage.fillNode(taskNode.getTaskIdNode(String.valueOf(taskId)), task.toZk());

            JobExecutionEvent jobExecutionEvent = new JobExecutionEvent(taskId, JobStatusTraceEvent.State
                    .TASK_RECOVERY, ip);
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
            log.debug("taskId =  " + taskId + " 任务停止后， taskCount =  " + taskCount + "  条任务。");
        } catch (Exception ex) {
            ZkOperationExceptionHandler.handleException(ex);
        }
    }

    /**
     * 根据tasks获取孩子节点ID列表
     *
     * @return 任务Id列表
     */
    public List<String> getTaskIds() {
        List<String> result = new LinkedList<>();
        for (String each : nodeStorage.getNodeChildrenKeys(TaskNode.ROOT)) {
            result.add(each);
        }
        return result;
    }

    /**
     * 根据任务id获取任务对象
     *
     * @return
     */
    public Task getTaskById(String taskId) {
        String nodeData = nodeStorage.getNodeData(taskNode.getTaskIdNode(taskId));

        Task task = null;
        try {
            task = Task.fromZk(nodeData);
        } catch (ClassNotFoundException e) {

        }

        return task;
    }

    /**
     * 生成/tasks 任务节点
     */
    private void persistOnline() {
        nodeStorage.fillNode(taskNode.getTasksNode(), "");

    }


    /**
     * 停止任务，只能停止没有运行的任务
     */
    @RequiredArgsConstructor
    class StopTaskTransactionExecutionCallback implements TransactionExecutionCallback {

        final long taskId;
        String outAndLocalIp = IPUtils.getUniqueServerId();

        @Override
        public void execute(final CuratorTransactionFinal curatorTransactionFinal) throws Exception {
            /**   delete  /servers/169.254.79.228_10.0.75.1/tasks/running/111    里删除一个任务
             *    数据库更新任务为完成
             */

            //运行任务不能停止
            curatorTransactionFinal.delete().forPath(nodePath.getFullPath(TaskNode.getTaskIdNode(String.valueOf(taskId)
            ))).and();
//            curatorTransactionFinal.delete().forPath(nodePath.getFullPath(ServerNode.getWaitingTaskIdNode
//                    (outAndLocalIp, taskId
//                    ))).and();
        }

        @Override
        public void afterCommit() throws Exception {
            String ip = IPUtils.getUniqueServerId();

            JobExecutionEvent jobExecutionEvent = new JobExecutionEvent(taskId, JobStatusTraceEvent.State
                    .TASK_STOPPED, ip);
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
            log.debug("taskId =  " + taskId + " 任务停止后， taskCount =  " + taskCount + "  条任务。");
        }
    }

}
