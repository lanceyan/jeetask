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
import com.jeeframework.jeetask.zookeeper.listener.ElectionListenerManager;
import com.jeeframework.jeetask.zookeeper.server.ServerService;
import com.jeeframework.jeetask.zookeeper.storage.LeaderExecutionCallback;
import com.jeeframework.jeetask.zookeeper.storage.NodeStorage;
import com.jeeframework.jeetask.zookeeper.task.TaskService;
import com.jeeframework.util.validate.Validate;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.springframework.beans.factory.BeanFactory;

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

    private final AssignmentWorker assignmentWorker;

    private final ElectionListenerManager electionListenerManager;

    private final String roles;


    public LeaderService(final CoordinatorRegistryCenter regCenter, final JobEventBus jobEventBus, final BeanFactory
            context, final ServerService serverService, final String roles) {
        nodeStorage = new NodeStorage(regCenter);
        this.serverService = serverService;
        taskService = new TaskService(regCenter, jobEventBus);
        assignmentWorker = new AssignmentWorker();
        electionListenerManager = new ElectionListenerManager(regCenter, this, serverService);
        new Thread(assignmentWorker).start();
        electionListenerManager.start();//启动监听选举节点监听器
        this.roles = roles;
    }

    /**
     * 选举主节点.
     */
    public void electLeader() {
        log.debug("Elect a new leader now.");
        nodeStorage.executeInLeader(LeaderNode.LATCH, new LeaderElectionExecutionCallback());

        log.debug("Leader election completed. Leader is  " + nodeStorage.getNodeData(LeaderNode.INSTANCE));
    }

    public void stopAssignmentWork() {
        assignmentWorker.stop();
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

            if (serverService.isAvailableServer(IPUtils.getUniqueServerId())) {
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
        return IPUtils.getUniqueServerId().equals(getIp(nodeStorage.getNodeData(LeaderNode.INSTANCE)));
//                !JobRegistry.getInstance().isShutdown(this.jobName) && JobRegistry.getInstance()
//                .getJobInstance(this.jobName)
//                .getJobInstanceId().equals(nodeStorage.getNodeData(LeaderNode.INSTANCE));
    }

    /**
     * 判断是否已经有主节点.
     *
     * @return 是否已经有主节点
     */
    public boolean hasLeader() {
        return nodeStorage.isNodeExisted(LeaderNode.INSTANCE);
    }

    /**
     * 删除主节点供重新选举.
     */
    public void removeLeader() {
        nodeStorage.removeNodeIfExisted(LeaderNode.INSTANCE);
    }

    @RequiredArgsConstructor
    class LeaderElectionExecutionCallback implements LeaderExecutionCallback {

        @Override
        public void execute() {
            //通过没有leader，设定zookeeper领导节点选举成功
            //LeaderLatch.await();
            if (!hasLeader()) {
                try {
                    nodeStorage.fillEphemeralNode(LeaderNode.INSTANCE, makeLeaderInstance());

                } catch (Exception e) {
                    log.debug("竞争成为leader节点失败，错误为：" + e);
                }
            }
        }
    }


    public String makeLeaderInstance() {
        return IPUtils.getUniqueServerId() + DELIMITER + DateFormatUtils.format(new Date(), "yyyyMMddHHmmss");
    }

    /**
     * 获取作业服务器IP地址.
     *
     * @return 作业服务器IP地址
     */
    public String getIp(String leaderInstance) {
        if (Validate.isEmpty(leaderInstance)) {
            return "";
        }
        return leaderInstance.substring(0, leaderInstance.indexOf(DELIMITER));
    }


    /**
     * 分配工人worker
     */
    public class AssignmentWorker implements Runnable {
        public boolean allowStop = false;

        public void stop() {
            allowStop = true;
        }

        @Override
        public void run() {
            allowStop = false;
            while (!hasLeader() && serverService.hasAvailableServers()) {
                log.info("Leader is electing, waiting for {} ms", 100);
                BlockUtils.waitingShortTime();
            }
            assignTasks();
        }

        private void assignTasks() {
            //如果是主节点，主节点执行任务分配，要在循环里判断是否是主节点，防止当前节点断线，其他机器又被选举为主节点，任务被分配多次
            while (true) {
                String batchNo = DateFormatUtils.format(new Date(), "yyyyMMddHHmmss");
                try {
                    if (!allowStop && isLeader()) {
                        List<String> taskIdList = taskService.getTaskIds();
                        log.debug("当前批次： " + batchNo + "  任务分配开始，待分配有 " + taskIdList.size() + "  条任务。");

                        int assignCount = 0;
                        if (!Validate.isEmpty(taskIdList)) {

                            boolean isWorkers = roles.contains("worker");

                            List<Server> availableServers = serverService.getAvailableServers(isWorkers);

                            if (!Validate.isEmpty(availableServers)) {
                                for (String taskId : taskIdList) {
//                                    if (!isLeader()) {
//                                        log.debug("当前批次： " + batchNo + "  当前服务器不是leader节点停止分配任务 ");
//                                        break;
//                                    }
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
                                        assignCount++;
                                    }
                                }
                            }
                        }
                        log.debug("当前批次： " + batchNo + "  任务分配完成，分配了 " + assignCount + "  条任务。");

                    } else {

                        boolean isLeader = isLeader();
                        log.debug("当前节点停止分配任务  allowStop =  " + allowStop + " isLeader() = " + isLeader + "  leader =" +
                                " " + nodeStorage.getNodeData(LeaderNode.INSTANCE) + "。");
                        if (!hasLeader()) {
                            log.debug("当前节点停止分配任务  allowStop =  " + allowStop + " hasLeader() = " + hasLeader() + "， " +
                                    "发起主动选举。");
                            electLeader();
                        }


                    }
                } catch (Exception e) {
                    log.debug("当前批次： " + batchNo + "  任务分配出错，错误= " + e);
                }

                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                }
            }

        }
    }
}
