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

package com.jeeframework.jeetask.zookeeper.server;

import com.dangdang.ddframe.job.lite.internal.instance.InstanceNode;
import com.dangdang.ddframe.job.reg.base.CoordinatorRegistryCenter;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.jeeframework.jeetask.event.JobEventBus;
import com.jeeframework.jeetask.event.type.JobExecutionEvent;
import com.jeeframework.jeetask.event.type.JobStatusTraceEvent;
import com.jeeframework.jeetask.executor.ExecutorShutdownException;
import com.jeeframework.jeetask.server.Server;
import com.jeeframework.jeetask.task.Task;
import com.jeeframework.jeetask.util.net.IPUtils;
import com.jeeframework.jeetask.zookeeper.instance.InstanceService;
import com.jeeframework.jeetask.zookeeper.storage.NodePath;
import com.jeeframework.jeetask.zookeeper.storage.NodeStorage;
import com.jeeframework.jeetask.zookeeper.storage.TransactionExecutionCallback;
import com.jeeframework.jeetask.zookeeper.task.TaskNode;
import com.jeeframework.jeetask.zookeeper.worker.Worker;
import com.jeeframework.jeetask.zookeeper.worker.WorkerManager;
import com.jeeframework.util.validate.Validate;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.transaction.CuratorTransactionFinal;
import org.apache.curator.framework.recipes.atomic.DistributedAtomicInteger;
import org.apache.curator.retry.RetryNTimes;
import org.springframework.beans.factory.BeanFactory;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * 作业服务器服务.
 *
 * @author zhangliang
 * @author caohao
 */
@Slf4j
public final class ServerService {


    private final NodeStorage nodeStorage;
    private final ServerNode serverNode;
    private final NodePath nodePath;
    private final JobEventBus jobEventBus;
    private final ExecutorService workerManagerExecutorService;
    private final CoordinatorRegistryCenter regCenter;
    private final BeanFactory context;
    private final InstanceService instanceService;
    private final int maxAllowedWorkerCount;

    public ServerService(final CoordinatorRegistryCenter regCenter, final JobEventBus
            jobEventBus, final BeanFactory context, final int maxAllowedWorkerCount) {
        this.regCenter = regCenter;
        nodeStorage = new NodeStorage(regCenter);
        this.jobEventBus = jobEventBus;
        serverNode = new ServerNode();
        nodePath = new NodePath();
        workerManagerExecutorService = Executors.newFixedThreadPool(1);
        this.context = context;
        this.maxAllowedWorkerCount = maxAllowedWorkerCount;

        instanceService = new InstanceService(regCenter, jobEventBus, context, this);
    }

    public void updateStatus(final boolean enabled) {
        JsonObject json = new JsonObject();
        json.addProperty("status", enabled ? ServerStatus.ENABLED.name() : ServerStatus
                .DISABLED.name());
        int cpuCount = Runtime.getRuntime().availableProcessors();

        json.addProperty("maxAllowedWorkerCount", maxAllowedWorkerCount);

        json.addProperty("cpu", cpuCount);
//        long totalMemory = Runtime.getRuntime().totalMemory();
//        long freeMemory = Runtime.getRuntime().freeMemory();
//        long maxMemory = Runtime.getRuntime().maxMemory();

//        {"status":enabled ,  "cpu": 2, "load": 2,  "mem": {  "total" : "8g", "used": "2g" }}

//        OperatingSystemMXBean osmxb = (OperatingSystemMXBean) ManagementFactory
//                .getOperatingSystemMXBean();


        //填充服务器节点
        nodeStorage.fillNode(serverNode.getServerNode(IPUtils.getUniqueServerId()), json.toString()
        );
    }

    /**
     * 持久化作业服务器上线信息.
     */
    public void register(String roles) {

        updateStatus(true);
        //填充服务器下的任务节点
        nodeStorage.fillNode(serverNode.getTaskNode(IPUtils.getUniqueServerId()), "");

        //填充服务器下的等待任务节点
        nodeStorage.fillNode(serverNode.getWaitingTaskNode(IPUtils.getUniqueServerId()), "");

        //填充服务器下的运行中任务节点
        nodeStorage.fillNode(serverNode.getRunningTaskNode(IPUtils.getUniqueServerId()), "");

        if (roles.contains("worker")) {
            System.out.println("===============配置了worker角色，启动工作者线程==================");
            startWorkerManager();
        }

        instanceService.connect();
    }

    /**
     * 启动工人管理
     */
    private void startWorkerManager() {
        if (workerManagerExecutorService.isShutdown()) {
            throw new ExecutorShutdownException("创建workerManager线程池出错，excutorService关闭");
        }
        WorkerManager workerManager = new WorkerManager(this.regCenter, this.jobEventBus, context, this
                .maxAllowedWorkerCount,
                this);
        workerManagerExecutorService.execute(workerManager);
    }

    /**
     * 获取是否还有可用的作业服务器.
     *
     * @return 是否还有可用的作业服务器
     */
    public boolean hasAvailableServers() {
        List<String> servers = nodeStorage.getNodeChildrenKeys(ServerNode.ROOT);
        for (String each : servers) {
            if (isAvailableServer(each)) {
                return true;
            }
        }
        return false;
    }

    /**
     * 判断作业服务器是否可用.
     *
     * @param ip 作业服务器IP地址
     * @return 作业服务器是否可用
     */
    public boolean isAvailableServer(final String ip) {
        return isEnableServer(ip) && hasOnlineInstances(ip);
    }

    private boolean hasOnlineInstances(final String ip) {
        for (String each : nodeStorage.getNodeChildrenKeys(InstanceNode.ROOT)) {
            if (each.startsWith(ip)) {
                return true;
            }
        }
        return false;
    }

    /**
     * 判断服务器是否启用.
     *
     * @param ip 作业服务器IP地址
     * @return 服务器是否启用
     */
    public boolean isEnableServer(final String ip) {
        JsonObject serverStatusJSON = getServerStatusJSONByIp(ip);

        return verifyServerStatus(serverStatusJSON);
    }

    private boolean verifyServerStatus(JsonObject serverStatusJSON) {
        String serverStatus = serverStatusJSON.get("status").getAsString();
        return ServerStatus.ENABLED.name().equals(serverStatus);
    }

    /**
     * 根据IP获取当前server的状态
     *
     * @param ip IP
     * @return {"status":enabled , "maxAllowedWorkerCount":"",  "cpu": 2, "load": 2,  "mem": {  "total" : "8g", "used":
     * "2g" }}
     */
    private JsonObject getServerStatusJSONByIp(String ip) {
        return new JsonParser().parse(nodeStorage.getNodeData(serverNode.getServerNode(ip)))
                .getAsJsonObject();
    }

    /**
     * 对服务器分配任务
     *
     * @param server
     * @param task
     */
    public void assignTask(Server server, Task task) {
        //保证在一个事务里完成
        nodeStorage.executeInTransaction(new AssignTaskTransactionExecutionCallback(server, task));
    }

    /**
     * 认领任务
     *
     * @param task
     */
    public void claimTask(Task task, ExecutorService workerExecutorService) {
        //保证在一个事务里完成
        nodeStorage.executeInTransaction(new ClaimTaskTransactionExecutionCallback(task, workerExecutorService, this));
    }

    /**
     * 返回可用的服务器列表
     *
     * @return serverList
     */
    public List<Server> getAvailableServers() {
        List<String> servers = nodeStorage.getNodeChildrenKeys(ServerNode.ROOT);

        List<Server> availableServerList = new ArrayList<Server>();

        for (String ip : servers) {
            JsonObject serverStatusJSON = getServerStatusJSONByIp(ip);
            int maxAllowedWorkerCount = serverStatusJSON.get("maxAllowedWorkerCount").getAsInt();

            Server server = new Server();
            server.setOutAndLocalIp(ip);
            server.setTaskCount(0);
            server.setMaxAllowedWorkerCount(maxAllowedWorkerCount);


//{"status":enabled ,  "cpu": 2, "load": 2,  "mem": {  "total" : "8g", "used": "2g" }}
            if (verifyServerStatus(serverStatusJSON) && hasOnlineInstances(ip)) {
                //如果服务可用，检查服务器状态和当前的任务数，计算出可以分配的分值
                //       /servers/169.254.79.228_10.0.75.1/taskCounts
                String taskCountTmp = nodeStorage.getNodeData(serverNode.getTaskCountNode(ip));
                if (!Validate.isEmpty(taskCountTmp)) {
                    int taskCount = 0;
                    try {
                        taskCount = Integer.parseInt(taskCountTmp);
                        server.setTaskCount(taskCount);
                    } catch (NumberFormatException e) {
                    }
                }
                availableServerList.add(server);
            }
        }
        List<Server> retServerList = null;
        if (!Validate.isEmpty(availableServerList)) {
            retServerList = computeServerListByPerf(availableServerList);
        }

        return retServerList;
    }

    /**
     * 根据每个机器的cpu和现在运行的任务数得到一个排序后的list，逐个分配，任务超过负荷的不分配
     *
     * @param availableServerList
     * @return
     */
    private List<Server> computeServerListByPerf(List<Server> availableServerList) {
        List<Server> serverList = new ArrayList<>();

        for (Server server : availableServerList) {
            int cpuCount = server.getCpuCount();
            int taskCount = server.getTaskCount();
            int maxAllowedWorkerCount = server.getMaxAllowedWorkerCount();

            if (maxAllowedWorkerCount <= 0) {
                maxAllowedWorkerCount = cpuCount + 1;
            }
            if (taskCount < maxAllowedWorkerCount) {
                serverList.add(server);
            }
        }

        //筛选了任务后，按照taskCount 升序排序,优先分配任务数少的节点
        Collections.sort(serverList, new Comparator<Object>() {
            public int compare(Object o1, Object o2) {
                //你首先设置你要比较的东西
                Server s1 = (Server) o1;
                Server s2 = (Server) o2;
                if (s1.getTaskCount() > s2.getTaskCount()) {
                    return 1;
                }
                //小于同理
                if (s1.getTaskCount() < s2.getTaskCount()) {
                    return -1;
                }
                //如果返回0则认为前者与后者相等
                return 0;
            }
        });

        return serverList;
    }

    @RequiredArgsConstructor
    class AssignTaskTransactionExecutionCallback implements TransactionExecutionCallback {

        final Server server;
        final Task task;

        String outAndLocalIp = IPUtils.getUniqueServerId();

        @Override
        public void execute(final CuratorTransactionFinal curatorTransactionFinal) throws Exception {
            long taskId = task.getId();
//            serverService.assignTask(serverTmp, taskTmp);
//            taskService.deleteTask(taskId);

            /**    /servers/169.254.79.228_10.0.75.1/taskCounts    原子锁计数 + 1
             *    这是任务状态为待分配，同时更新数据库任务状态为待分配
             *    分配到具体服务器里  delete /tasks/1             add   /servers/169.254.79.228_10.0.75.1/tasks/1
             */


            //服务器节点添加任务节点
            curatorTransactionFinal.create().forPath(nodePath.getFullPath(ServerNode.getWaitingTaskIdNode(outAndLocalIp,
                    taskId)), task.toZk().getBytes()).and();
            //原任务队列删除
            curatorTransactionFinal.delete().forPath(nodePath.getFullPath(TaskNode.getTaskIdNode(String.valueOf(taskId)
            ))).and();


        }

        @Override
        public void afterCommit() throws Exception {

            long taskId = task.getId();
            String ip = IPUtils.getUniqueServerId();
            JobExecutionEvent jobExecutionEvent = new JobExecutionEvent(taskId, JobStatusTraceEvent.State
                    .TASK_STAGING, ip);
            jobEventBus.trigger(jobExecutionEvent);

            //服务器上任务计数+1
            CuratorFramework client = nodeStorage.getClient();
            DistributedAtomicInteger counter = new DistributedAtomicInteger(client, nodePath.getFullPath(ServerNode
                    .getTaskCountNode
                            (outAndLocalIp)), new
                    RetryNTimes(100, 1000));
            counter.increment();

            int taskCount = counter.get().postValue();
            log.debug("taskId =  " + taskId + "任务分配成功， taskCount =  " + taskCount + "  条任务。");
        }
    }

    @RequiredArgsConstructor
    class ClaimTaskTransactionExecutionCallback implements TransactionExecutionCallback {

        final Task task;
        final ExecutorService workerExecutorService;
        final ServerService serverService;


//            Gson gson = new Gson();
//            String taskJSON = gson.toJson(task);

        String outAndLocalIp = IPUtils.getUniqueServerId();

        @Override
        public void execute(final CuratorTransactionFinal curatorTransactionFinal) throws Exception {
            long taskId = task.getId();
            /**   delete  /servers/169.254.79.228_10.0.75.1/tasks/waiting/111    里删除一个任务
             *    add     /servers/169.254.79.228_10.0.75.1/tasks/running/111  里添加一个任务
             */
            //waiting任务队列删除
            curatorTransactionFinal.delete().forPath(nodePath.getFullPath(ServerNode.getWaitingTaskIdNode
                    (outAndLocalIp, taskId
                    ))).and();
            //running添加任务节点
            curatorTransactionFinal.create().forPath(nodePath.getFullPath(ServerNode.getRunningTaskIdNode(outAndLocalIp,
                    taskId)), task.toZk().getBytes()).and();


            log.debug("taskId =  " + taskId + "  放入了running队列，准备执行啦！");
        }

        @Override
        public void afterCommit() throws Exception {

            workerExecutorService.execute(new Worker(regCenter, jobEventBus, task, context, serverService));
        }
    }

    /**
     * 获取等待执行的任务节点ID列表
     *
     * @return 任务Id列表
     */
    public List<String> getWaitingTaskIds() {
        List<String> result = new LinkedList<>();
        String ip = IPUtils.getUniqueServerId();
        for (String each : nodeStorage.getNodeChildrenKeys(ServerNode.getWaitingTaskNode(ip))) {
            result.add(each);
        }
        return result;
    }

    /**
     * 根据任务id获取正在等待的任务对象
     *
     * @return
     */
    public Task getWaitingTaskById(long taskId) {
        String ip = IPUtils.getUniqueServerId();
        String nodeData = nodeStorage.getNodeData(ServerNode.getWaitingTaskIdNode(ip, taskId));
        Task task = null;
        try {
            task = Task.fromZk(nodeData);
        } catch (ClassNotFoundException e) {

        }

        return task;
    }

    /**
     * 获取正在运行执行的任务节点ID列表
     *
     * @return 任务Id列表
     */
    public List<String> getRunningTaskIds() {
        List<String> result = new LinkedList<>();
        String ip = IPUtils.getUniqueServerId();
        for (String each : nodeStorage.getNodeChildrenKeys(ServerNode.getRunningTaskNode(ip))) {
            result.add(each);
        }
        return result;
    }

    /**
     * 根据任务id获取正在运行的任务对象
     *
     * @return
     */
    public Task getRunningTaskById(long taskId) {
        String ip = IPUtils.getUniqueServerId();
        String nodeData = nodeStorage.getNodeData(ServerNode.getRunningTaskIdNode(ip, taskId));
        Task task = null;
        try {
            task = Task.fromZk(nodeData);
        } catch (ClassNotFoundException e) {

        }

        return task;
    }


}


