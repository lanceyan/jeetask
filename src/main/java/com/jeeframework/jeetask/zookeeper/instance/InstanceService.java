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

package com.jeeframework.jeetask.zookeeper.instance;


import com.dangdang.ddframe.job.lite.api.strategy.JobInstance;
import com.dangdang.ddframe.job.reg.base.CoordinatorRegistryCenter;
import com.jeeframework.jeetask.event.JobEventBus;
import com.jeeframework.jeetask.zookeeper.server.ServerService;
import com.jeeframework.jeetask.zookeeper.storage.NodeStorage;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.springframework.beans.factory.BeanFactory;

import java.util.Date;
import java.util.LinkedList;
import java.util.List;

/**
 * 作业运行实例服务.
 *
 * @author zhangliang
 */
public final class InstanceService {

    private final NodeStorage nodeStorage;

    private final InstanceNode instanceNode;

    private final ServerService serverService;

    public InstanceService(final CoordinatorRegistryCenter regCenter, final JobEventBus jobEventBus, final
    BeanFactory context, final ServerService serverService) {
        nodeStorage = new NodeStorage(regCenter);
        instanceNode = new InstanceNode();
        this.serverService = serverService;
    }

    /**
     * 持久化作业运行实例上线相关信息.
     */
    public void connect() {
        nodeStorage.fillEphemeralNode(instanceNode.getLocalInstanceNode(), DateFormatUtils.format(new Date(),
                "yyyyMMddHHmmss"));
    }

    /**
     * 删除作业运行状态.
     */
    public void removeInstance() {
        nodeStorage.removeNodeIfExisted(instanceNode.getLocalInstanceNode());
    }


    /**
     * 获取可分片的作业运行实例.
     *
     * @return 可分片的作业运行实例
     */
    public List<JobInstance> getAvailableJobInstances() {
        List<JobInstance> result = new LinkedList<>();
        for (String each : nodeStorage.getNodeChildrenKeys(InstanceNode.ROOT)) {
            JobInstance jobInstance = new JobInstance(each);
            if (serverService.isEnableServer(jobInstance.getIp())) {
                result.add(new JobInstance(each));
            }
        }
        return result;
    }

    /**
     * 判断当前作业运行实例的节点是否仍然存在.
     *
     * @return 当前作业运行实例的节点是否仍然存在
     */
    public boolean isLocalJobInstanceExisted() {
        return nodeStorage.isNodeExisted(instanceNode.getLocalInstanceNode());
    }
}
