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
import com.google.gson.Gson;
import com.jeeframework.jeetask.event.JobEventBus;
import com.jeeframework.jeetask.event.rdb.JobEventStorage;
import com.jeeframework.jeetask.event.type.JobExecutionEvent;
import com.jeeframework.jeetask.task.Task;
import com.jeeframework.jeetask.zookeeper.storage.NodeStorage;

import java.util.LinkedList;
import java.util.List;

/**
 * 作业服务器服务.
 *
 * @author lance
 */
public final class TaskService {


    private final NodeStorage nodeStorage;
    private final TaskNode taskNode;
    private JobEventBus jobEventBus;

    public TaskService(final CoordinatorRegistryCenter regCenter, final JobEventBus jobEventBus) {
        nodeStorage = new NodeStorage(regCenter);
        taskNode = new TaskNode();
        this.jobEventBus = jobEventBus;
    }


    /**
     * 持久化任务信息
     *
     * @param task
     */
    public void submitTask(Task task) {
        JobExecutionEvent jobExecutionEvent = new JobExecutionEvent(task);
//        jobEventBus.post(jobExecutionEvent);
        JobEventStorage repository = jobEventBus.getJobEventListener().getRepository();

        //实时同步触发事件
        jobEventBus.trigger(jobExecutionEvent);

        task.setId(jobExecutionEvent.getTaskId());

        long taskId = task.getId();
        Gson gson = new Gson();
        String taskJSON = gson.toJson(task);
        nodeStorage.fillJobNode(taskNode.getTaskIdNode(String.valueOf(taskId)), taskJSON);


    }

    /**
     * 根据tasks获取孩子节点ID列表
     *
     * @return 任务Id列表
     */
    public List<String> getTaskIds() {
        List<String> result = new LinkedList<>();
        for (String each : nodeStorage.getJobNodeChildrenKeys(TaskNode.ROOT)) {
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
        String nodeData = nodeStorage.getJobNodeData(taskNode.getTaskIdNode(taskId));
        Gson gson = new Gson();
        Task task = gson.fromJson(nodeData, Task.class);

        return task;
    }

    /**
     * 生成/tasks 任务节点
     */
    public void persistOnline() {
        nodeStorage.fillJobNode(taskNode.getTasksNode(), "");

    }

}
