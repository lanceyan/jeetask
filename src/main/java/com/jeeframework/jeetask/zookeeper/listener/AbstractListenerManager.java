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

package com.jeeframework.jeetask.zookeeper.listener;

import com.dangdang.ddframe.job.reg.base.CoordinatorRegistryCenter;
import com.jeeframework.jeetask.zookeeper.storage.NodeStorage;
import org.apache.curator.framework.recipes.cache.TreeCacheListener;

/**
 * 作业注册中心的监听器管理者的抽象类.
 *
 * @author zhangliang
 */
public abstract class AbstractListenerManager {

    private final NodeStorage nodeStorage;

    protected AbstractListenerManager(final CoordinatorRegistryCenter regCenter, final String jobName) {
        nodeStorage = new NodeStorage(regCenter);
    }

    /**
     * 开启监听器.
     */
    public abstract void start();

    protected void addDataListener(final String path, final TreeCacheListener listener) {
        nodeStorage.addDataListener(path, listener);
    }
}
