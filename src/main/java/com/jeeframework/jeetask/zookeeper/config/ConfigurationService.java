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

package com.jeeframework.jeetask.zookeeper.config;


import com.dangdang.ddframe.job.exception.JobConfigurationException;
import com.dangdang.ddframe.job.exception.JobExecutionEnvironmentException;
import com.dangdang.ddframe.job.reg.base.CoordinatorRegistryCenter;
import com.dangdang.ddframe.job.util.env.TimeService;
import com.google.common.base.Optional;
import com.jeeframework.jeetask.config.simple.JobConfiguration;
import com.jeeframework.jeetask.zookeeper.storage.NodeStorage;

/**
 * 弹性化分布式作业配置服务.
 * 
 * @author zhangliang
 * @author caohao
 */
public final class ConfigurationService {
    
    private final TimeService timeService;
    
    private final NodeStorage nodeStorage;
    
    public ConfigurationService(final CoordinatorRegistryCenter regCenter ) {
        nodeStorage = new NodeStorage(regCenter );
        timeService = new TimeService();
    }
    
    /**
     * 读取作业配置.
     * 
     * @param fromCache 是否从缓存中读取
     * @return 作业配置
     */
    public JobConfiguration load(final boolean fromCache) {
        String result;
        if (fromCache) {
            result = nodeStorage.getJobNodeData(ConfigurationNode.ROOT);
            if (null == result) {
                result = nodeStorage.getJobNodeDataDirectly(ConfigurationNode.ROOT);
            }
        } else {
            result = nodeStorage.getJobNodeDataDirectly(ConfigurationNode.ROOT);
        }
        return LiteJobConfigurationGsonFactory.fromJson(result);
    }

    /**
     * 持久化分布式作业配置信息.
     *
     * @param liteJobConfig 作业配置
     */
    public void persist(final JobConfiguration liteJobConfig) {
        checkConflictJob(liteJobConfig);
        if (!nodeStorage.isJobNodeExisted(ConfigurationNode.ROOT) || liteJobConfig.isOverwrite()) {
            nodeStorage.replaceJobNode(ConfigurationNode.ROOT, LiteJobConfigurationGsonFactory.toJson(liteJobConfig));
        }
    }

    private void checkConflictJob(final JobConfiguration liteJobConfig) {
        Optional<JobConfiguration> liteJobConfigFromZk = find();
        if (liteJobConfigFromZk.isPresent() && !liteJobConfigFromZk.get().getJobDefinition().getJobClass().equals(liteJobConfig.getJobDefinition().getJobClass())) {
            throw new JobConfigurationException("Job conflict with register center. The job '%s' in register center's class is '%s', your job class is '%s'",
                    liteJobConfig.getJobName(), liteJobConfigFromZk.get().getJobDefinition().getJobClass(), liteJobConfig.getJobDefinition().getJobClass());
        }
    }

    private Optional<JobConfiguration> find() {
        if (!nodeStorage.isJobNodeExisted(ConfigurationNode.ROOT)) {
            return Optional.absent();
        }
        JobConfiguration result = LiteJobConfigurationGsonFactory.fromJson(nodeStorage.getJobNodeDataDirectly(ConfigurationNode.ROOT));
        if (null == result) {
            // TODO 应该删除整个job node,并非仅仅删除config node
            nodeStorage.removeJobNodeIfExisted(ConfigurationNode.ROOT);
        }
        return Optional.fromNullable(result);
    }
    
    /**
     * 检查本机与注册中心的时间误差秒数是否在允许范围.
     * 
     * @throws JobExecutionEnvironmentException 本机与注册中心的时间误差秒数不在允许范围所抛出的异常
     */
    public void checkMaxTimeDiffSecondsTolerable() throws JobExecutionEnvironmentException {
        int maxTimeDiffSeconds =  load(true).getMaxTimeDiffSeconds();
        if (-1  == maxTimeDiffSeconds) {
            return;
        }
        long timeDiff = Math.abs(timeService.getCurrentMillis() - nodeStorage.getRegistryCenterTime());
        if (timeDiff > maxTimeDiffSeconds * 1000L) {
            throw new JobExecutionEnvironmentException(
                    "Time different between job server and register center exceed '%s' seconds, max time different is '%s' seconds.", timeDiff / 1000, maxTimeDiffSeconds);
        }
    }
}
