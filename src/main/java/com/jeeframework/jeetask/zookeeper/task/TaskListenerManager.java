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
import com.jeeframework.jeetask.zookeeper.listener.AbstractJobListener;
import com.jeeframework.jeetask.zookeeper.listener.AbstractListenerManager;
import com.jeeframework.jeetask.zookeeper.server.ServerService;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent.Type;

/**
 * 任务节点监听管理器.
 *
 * @author lance
 */
public final class TaskListenerManager extends AbstractListenerManager {


    private   ServerService serverService;

    public TaskListenerManager(final CoordinatorRegistryCenter regCenter, final String jobName) {
        super(regCenter, jobName);
//        serverService = new ServerService(regCenter);
    }

    @Override
    public void start() {
        addDataListener("/tasks", new TaskAssignJobListener());
    }

    /**
     * 任务分配监听器
     */
    class TaskAssignJobListener extends AbstractJobListener {

        @Override
        protected void dataChanged(final String path, final Type eventType, final String data) {
            if (true) {


            }
        }


    }

}
