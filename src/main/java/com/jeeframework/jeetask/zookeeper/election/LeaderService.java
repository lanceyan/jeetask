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

package com.jeeframework.jeetask.zookeeper.election;

import com.dangdang.ddframe.job.reg.base.CoordinatorRegistryCenter;
import com.dangdang.ddframe.job.util.concurrent.BlockUtils;
import com.jeeframework.jeetask.event.JobEventBus;
import com.jeeframework.jeetask.server.Server;
import com.jeeframework.jeetask.task.Task;
import com.jeeframework.jeetask.util.net.IPUtils;
import com.jeeframework.jeetask.zookeeper.server.ServerService;
import com.jeeframework.jeetask.zookeeper.storage.LeaderExecutionCallback;
import com.jeeframework.jeetask.zookeeper.storage.NodeStorage;
import com.jeeframework.jeetask.zookeeper.task.TaskService;
import com.jeeframework.util.validate.Validate;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.time.DateFormatUtils;

import java.util.Date;
import java.util.List;

/**
 * 主节点服务.
 *
 * @author zhangliang
 */
@Slf4j
public final class LeaderService {

    private static final String DELIMITER = "@-@";

    private final ServerService serverService;
    private final TaskService taskService;

    private final NodeStorage nodeStorage;


    public LeaderService(final CoordinatorRegistryCenter regCenter, final JobEventBus jobEventBus) {
        nodeStorage = new NodeStorage(regCenter);
        serverService = new ServerService(regCenter, jobEventBus);
        taskService = new TaskService(regCenter, jobEventBus);
    }

    /**
     * 选举主节点.
     */
    public void electLeader() {
        log.debug("Elect a new leader now.");
        nodeStorage.executeInLeader(LeaderNode.LATCH, new LeaderElectionExecutionCallback());
        log.debug("Leader election completed. Leader is  " + nodeStorage.getJobNodeData(LeaderNode.INSTANCE));
    }

    /**
     * 判断当前节点是否是主节点.
     * <p>
     * <p>
     * 如果主节点正在选举中而导致取不到主节点, 则阻塞至主节点选举完成再返回.
     * </p>
     *
     * @return 当前节点是否是主节点
     */
    public boolean isLeaderUntilBlock() {
        while (!hasLeader() && serverService.hasAvailableServers()) {
            log.info("Leader is electing, waiting for {} ms", 100);
            BlockUtils.waitingShortTime();

            if (serverService.isAvailableServer(IPUtils.getOutAndLocalIPV4())) {
                electLeader();
            }

//            if (!JobRegistry.getInstance().isShutdown(this.jobName) && serverService.isAvailableServer
//                    (JobRegistry
//                            .getInstance().getJobInstance(this.jobName).getIp())) {
//                electLeader();
//            }
        }
        return isLeader();
    }

    /**
     * 判断当前节点是否是主节点.
     *
     * @return 当前节点是否是主节点
     */
    public boolean isLeader() {
        return IPUtils.getOutAndLocalIPV4().equals(getIp(nodeStorage.getJobNodeData(LeaderNode.INSTANCE)));
//                !JobRegistry.getInstance().isShutdown(this.jobName) && JobRegistry.getInstance()
//                .getJobInstance(this.jobName)
//                .getJobInstanceId().equals(nodeStorage.getJobNodeData(LeaderNode.INSTANCE));
    }

    /**
     * 判断是否已经有主节点.
     *
     * @return 是否已经有主节点
     */
    public boolean hasLeader() {
        return nodeStorage.isJobNodeExisted(LeaderNode.INSTANCE);
    }

    /**
     * 删除主节点供重新选举.
     */
    public void removeLeader() {
        nodeStorage.removeJobNodeIfExisted(LeaderNode.INSTANCE);
    }

    @RequiredArgsConstructor
    class LeaderElectionExecutionCallback implements LeaderExecutionCallback {

        @Override
        public void execute() {
            //通过没有leader，设定zookeeper领导节点选举成功
            //LeaderLatch.await();
            if (!hasLeader()) {
                nodeStorage.fillEphemeralJobNode(LeaderNode.INSTANCE, makeLeaderInstance());

                //如果是主节点，主节点执行任务分配
                while (true) {
                    String batchNo = DateFormatUtils.format(new Date(), "yyyyMMddHHmmss");
                    List<String> taskIdList = taskService.getTaskIds();
                    log.debug("当前批次： " + batchNo + "  任务分配开始，待分配有 " + taskIdList.size() + "  条任务。");
                    if (!Validate.isEmpty(taskIdList)) {

                        List<Server> availableServers = serverService.getAvailableServers();

                        if (!Validate.isEmpty(availableServers)) {
                            for (String taskId : taskIdList) {
                                Server serverTmp = null;
                                try {
                                    //有可能服务器挂掉，要判断为空，还有可能服务器为空，出现了failover的情况，需要注意分配不成功导致任务丢失
                                    serverTmp = availableServers.remove(0);
                                } catch (IndexOutOfBoundsException e) {
                                    break;//跳出分配
                                }

                                if (null != serverTmp) {
                                    Task taskTmp = taskService.getTaskById(taskId);

                                    //执行分配任务
                                    serverService.assignTask(serverTmp, taskTmp);
                                }
                            }
                        }
                    }
                    log.debug("当前批次： " + batchNo + "  任务分配完成，分配了 " + taskIdList.size() + "  条任务。");
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                    }
                }
            }
        }
    }


    public String makeLeaderInstance() {
        return IPUtils.getOutAndLocalIPV4() + DELIMITER + DateFormatUtils.format(new Date(), "yyyyMMddHHmmss");
    }

    /**
     * 获取作业服务器IP地址.
     *
     * @return 作业服务器IP地址
     */
    public String getIp(String leaderInstance) {
        return leaderInstance.substring(0, leaderInstance.indexOf(DELIMITER));
    }
}
