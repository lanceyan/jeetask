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

package com.jeeframework.jeetask.zookeeper.sharding;

import com.dangdang.ddframe.job.lite.api.strategy.JobInstance;
import com.dangdang.ddframe.job.lite.api.strategy.JobShardingStrategy;
import com.dangdang.ddframe.job.lite.api.strategy.JobShardingStrategyFactory;
import com.dangdang.ddframe.job.lite.internal.schedule.JobRegistry;
import com.dangdang.ddframe.job.reg.base.CoordinatorRegistryCenter;
import com.dangdang.ddframe.job.util.concurrent.BlockUtils;
import com.jeeframework.jeetask.config.simple.JobConfiguration;
import com.jeeframework.jeetask.zookeeper.config.ConfigurationService;
import com.jeeframework.jeetask.zookeeper.election.LeaderService;
import com.jeeframework.jeetask.zookeeper.instance.InstanceNode;
import com.jeeframework.jeetask.zookeeper.instance.InstanceService;
import com.jeeframework.jeetask.zookeeper.server.ServerService;
import com.jeeframework.jeetask.zookeeper.storage.JobNodePath;
import com.jeeframework.jeetask.zookeeper.storage.JobNodeStorage;
import com.jeeframework.jeetask.zookeeper.storage.TransactionExecutionCallback;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.api.transaction.CuratorTransactionFinal;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * 作业分片服务.
 *
 * @author zhangliang
 */
@Slf4j
public final class ShardingService {

    private   String jobName;

    private   JobNodeStorage jobNodeStorage;

    private   LeaderService leaderService;

    private   ConfigurationService configService;

    private   InstanceService instanceService;

    private   ServerService serverService;

    private   ExecutionService executionService;

    private   JobNodePath jobNodePath;

    public ShardingService(final CoordinatorRegistryCenter regCenter, final String jobName) {
//        this.jobName = jobName;
//        jobNodeStorage = new JobNodeStorage(regCenter, jobName);
//        leaderService = new LeaderService(regCenter);
//        configService = new ConfigurationService(regCenter, jobName);
//        instanceService = new InstanceService(regCenter, jobName);
//        serverService = new ServerService(regCenter);
//        executionService = new ExecutionService(regCenter, jobName);
//        jobNodePath = new JobNodePath(jobName);
    }

    /**
     * 设置需要重新分片的标记.
     */
    public void setReshardingFlag() {
        jobNodeStorage.createJobNodeIfNeeded(ShardingNode.NECESSARY);
    }

    /**
     * 判断是否需要重分片.
     *
     * @return 是否需要重分片
     */
    public boolean isNeedSharding() {
        return jobNodeStorage.isJobNodeExisted(ShardingNode.NECESSARY);
    }

    /**
     * 如果需要分片且当前节点为主节点, 则作业分片.
     * <p>
     * <p>
     * 如果当前无可用节点则不分片.
     * </p>
     */
    public void shardingIfNecessary() {
        List<JobInstance> availableJobInstances = instanceService.getAvailableJobInstances();
        if (!isNeedSharding() || availableJobInstances.isEmpty()) {
            return;
        }
        if (!leaderService.isLeaderUntilBlock()) {
            blockUntilShardingCompleted();
            return;
        }
        waitingOtherJobCompleted();
        JobConfiguration liteJobConfig = configService.load(false);
        int shardingTotalCount = 1;//liteJobConfig.getTypeConfig().getCoreConfig().getShardingTotalCount();
        log.debug("Job '{}' sharding begin.", jobName);
        jobNodeStorage.fillEphemeralJobNode(ShardingNode.PROCESSING, "");
        resetShardingInfo(shardingTotalCount);
        JobShardingStrategy jobShardingStrategy = JobShardingStrategyFactory.getStrategy(liteJobConfig
                .getJobShardingStrategyClass());
        jobNodeStorage.executeInTransaction(new PersistShardingInfoTransactionExecutionCallback(jobShardingStrategy
                .sharding(availableJobInstances, jobName, shardingTotalCount)));
        log.debug("Job '{}' sharding complete.", jobName);
    }

    private void blockUntilShardingCompleted() {
        while (!leaderService.isLeaderUntilBlock() && (jobNodeStorage.isJobNodeExisted(ShardingNode.NECESSARY) ||
                jobNodeStorage.isJobNodeExisted(ShardingNode.PROCESSING))) {
            log.debug("Job '{}' sleep short time until sharding completed.", jobName);
            BlockUtils.waitingShortTime();
        }
    }

    private void waitingOtherJobCompleted() {
        while (executionService.hasRunningItems()) {
            log.debug("Job '{}' sleep short time until other job completed.", jobName);
            BlockUtils.waitingShortTime();
        }
    }

    private void resetShardingInfo(final int shardingTotalCount) {
        for (int i = 0; i < shardingTotalCount; i++) {
            jobNodeStorage.removeJobNodeIfExisted(ShardingNode.getInstanceNode(i));
            jobNodeStorage.createJobNodeIfNeeded(ShardingNode.ROOT + "/" + i);
        }
        int actualShardingTotalCount = jobNodeStorage.getJobNodeChildrenKeys(ShardingNode.ROOT).size();
        if (actualShardingTotalCount > shardingTotalCount) {
            for (int i = shardingTotalCount; i < actualShardingTotalCount; i++) {
                jobNodeStorage.removeJobNodeIfExisted(ShardingNode.ROOT + "/" + i);
            }
        }
    }

    /**
     * 获取运行在本作业实例的分片项集合.
     *
     * @return 运行在本作业实例的分片项集合
     */
    public List<Integer> getLocalShardingItems() {
        if (JobRegistry.getInstance().isShutdown(jobName) || !serverService.isAvailableServer(JobRegistry.getInstance
                ().getJobInstance(jobName).getIp())) {
            return Collections.emptyList();
        }
        List<Integer> result = new LinkedList<>();
        int shardingTotalCount = 1;//configService.load(true).getTypeConfig().getCoreConfig().getShardingTotalCount();
        for (int i = 0; i < shardingTotalCount; i++) {
            if (JobRegistry.getInstance().getJobInstance(jobName).getJobInstanceId().equals(jobNodeStorage
                    .getJobNodeData(ShardingNode.getInstanceNode(i)))) {
                result.add(i);
            }
        }
        return result;
    }

    /**
     * 查询是包含有分片节点的不在线服务器.
     *
     * @return 是包含有分片节点的不在线服务器
     */
    public boolean hasShardingInfoInOfflineServers() {
        List<String> onlineInstances = jobNodeStorage.getJobNodeChildrenKeys(InstanceNode.ROOT);
        int shardingTotalCount = 1;//configService.load(true).getTypeConfig().getCoreConfig().getShardingTotalCount();
        for (int i = 0; i < shardingTotalCount; i++) {
            if (!onlineInstances.contains(jobNodeStorage.getJobNodeData(ShardingNode.getInstanceNode(i)))) {
                return true;
            }
        }
        return false;
    }

    @RequiredArgsConstructor
    class PersistShardingInfoTransactionExecutionCallback implements TransactionExecutionCallback {

        private final Map<JobInstance, List<Integer>> shardingResults;

        @Override
        public void execute(final CuratorTransactionFinal curatorTransactionFinal) throws Exception {
            for (Map.Entry<JobInstance, List<Integer>> entry : shardingResults.entrySet()) {
                for (int shardingItem : entry.getValue()) {
                    curatorTransactionFinal.create().forPath(jobNodePath.getFullPath(ShardingNode.getInstanceNode
                            (shardingItem)), entry.getKey().getJobInstanceId().getBytes()).and();
                }
            }
            curatorTransactionFinal.delete().forPath(jobNodePath.getFullPath(ShardingNode.NECESSARY)).and();
            curatorTransactionFinal.delete().forPath(jobNodePath.getFullPath(ShardingNode.PROCESSING)).and();
        }

        @Override
        public void afterCommit() throws Exception {

        }
    }
}
