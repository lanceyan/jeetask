/*
 * @project: jeetask 
 * @package: com.jeeframework.jeetask.zookeeper.schedule
 * @title:   JeeTaskServer.java 
 *
 * Copyright (c) 2017 jeeframework Limited, Inc.
 * All rights reserved.
 */
package com.jeeframework.jeetask.startup;


import com.jeeframework.jeetask.zookeeper.config.ConfigurationService;
import com.jeeframework.jeetask.zookeeper.election.LeaderService;
import com.jeeframework.jeetask.zookeeper.instance.InstanceService;
import com.jeeframework.jeetask.zookeeper.listener.RegistryCenterConnectionStateListener;
import com.jeeframework.jeetask.zookeeper.server.ServerService;
import com.jeeframework.jeetask.zookeeper.sharding.ShardingService;
import com.jeeframework.jeetask.zookeeper.storage.NodeStorage;
import com.jeeframework.jeetask.zookeeper.task.TaskService;
import com.jeeframework.util.validate.Validate;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import javax.sql.DataSource;
import java.io.IOException;

/**
 * jee任务调度框架的入口类
 *
 * @author lance
 * @version 1.0 2017-04-26 16:08
 */
@Slf4j
public class JeeTaskServer extends JeeTask {


    private LeaderService leaderService = null;
    private ServerService serverService = null;
    private InstanceService instanceService = null;
    private TaskService taskService = null;
    private ConfigurationService configService = null;
    private ShardingService shardingService = null;

    private String roles = "leader,worker"; //默认是leader、worker


    @Setter
    private String maxAllowedWorkerCount; //每个机器worker的数量


    public JeeTaskServer(DataSource taskDataSource) throws IOException {
        super(taskDataSource);
    }

    public JeeTaskServer() {
    }


    public void start() throws Exception {

        //配置文件环境变量
        String rolesTmp = null;
        try {
            rolesTmp = System.getProperty("roles");
        } catch (Exception e) {
        }
        if (Validate.isEmpty(rolesTmp)) {
            rolesTmp = roles;
            System.setProperty("roles", rolesTmp);
            System.out.println("===============没有配置  -Droles   环境变量，使用默认配置  " + rolesTmp + "  ==================");
        } else {
            System.out.println("===============找到 -Droles   系统环境变量，值为：  " + rolesTmp + "  ==================");
        }

        int workerNumInt = getMaxAllowedWorkerCount();

        serverService = new ServerService(regCenter, jobEventBus, context, workerNumInt);


        configService = new ConfigurationService(regCenter);
        taskService = new TaskService(regCenter, jobEventBus);


        /**
         * 1、注册服务器IP地址到zk
         *    路径    注册地址 key /servers/169.254.79.228_10.0.75.1  ，    {"status":enabled ,  "cpu": 2, "load": 2,  "mem": {
         *    "total" : "8g", "used": "2g" }}
         *
         *  2、添加当前服务器到 instance 路径节点去，注册地址   /instances/169.254.79.228_10.0.75.1@-@20170816131533
         *  outIP:innerIP@-@时间   (设定监控事件，instance断掉触发)
         *
         *  3、选举服务器leader  根据IP来选举，IP分为外网IP和内网IP，服务器内部使用内网IP通讯
         *    路径  先设定锁  /leader/election/latch
         *          在设定  key:  /leader/election/instance   ,   value:169.254.79.228_10.0.75.1@-@20170726151446
         *          选举leader成功
         *
         *  4、判断是不是主节点，如果是主节点根据任务队列进行分配任务到对应的服务器
         *    从
         *    /tasks    里查询获取需要处理的任务
         *
         *    /tasks/1
         *    /tasks/2
         *    /tasks/3
         *
         *    /servers/169.254.79.228_10.0.75.1/taskCounts    原子锁计数 + 1
         *
         *    这是任务状态为待分配，同时更新数据库任务状态为待分配
         *
         *    分配到具体服务器里  delete /tasks/1             add   /servers/169.254.79.228_10.0.75.1/tasks/1
         *
         *     /servers/169.254.79.228_10.0.75.1/tasks/1
         *     /servers/169.254.79.228_10.0.75.1/tasks/2
         *     /servers/169.254.79.228_10.0.75.1/tasks/3
         *
         *     这时任务状态为待启动
         *     /servers/169.254.79.228_10.0.75.1/tasks/1/status    0,1,2,3,9    0  待分配   1 待启动  2 运行中   3  完成  9 出错
         *  5、/servers  监听器监听  /servers/169.254.79.228/tasks/  下是否有新的任务，如果有就启动任务进行处理
         *
         *
         *     这时任务状态为运行中
         *
         *  6、运行完成或者出错，删除 /servers/169.254.79.228_10.0.75.1/tasks/1 的数据
         *  /servers/169.254.79.228_10.0.75.1/taskCounts    原子锁计数 - 1
         *
         */

        serverService.register(rolesTmp);
        NodeStorage nodeStorage = new NodeStorage(regCenter);
        RegistryCenterConnectionStateListener regCenterConnectionStateListener = new
                RegistryCenterConnectionStateListener(serverService);
        nodeStorage.addConnectionStateListener(regCenterConnectionStateListener);

        if (rolesTmp.contains("leader")) {
            System.out.println("===============配置了leader角色，启动选举监听线程==================");
            leaderService = new LeaderService(regCenter, jobEventBus, context, serverService, rolesTmp);
            leaderService.electLeader();
        }


    }

    /**
     * 获取执行工作线程数
     *
     * @return
     */
    protected int getMaxAllowedWorkerCount() {
        String workerNumTmp = null;
        try {
            workerNumTmp = System.getProperty("task.worker.maxAllowed");
        } catch (Exception e) {
        }
        if (Validate.isEmpty(workerNumTmp)) {
            workerNumTmp = properties.getProperty("task.worker.maxAllowed");
        }
        //properties里没有就判断通过spring方式注入有没有
        if (Validate.isEmpty(workerNumTmp)) {
            workerNumTmp = maxAllowedWorkerCount;
        }

        int workerNumInt = 0;//默认为0
        try {
            workerNumInt = Integer.valueOf(workerNumTmp);
        } catch (NumberFormatException e) {
        }
        return workerNumInt;
    }

}

//        shardingService = new ShardingService(regCenter);

//        JobNodeStorage jobNodeStorage = new JobNodeStorage(regCenter, jobName);
//        RegistryCenterConnectionStateListener regCenterConnectionStateListener = new
//                RegistryCenterConnectionStateListener(regCenter, jobName);
//
//        electionListenerManager = new ElectionListenerManager(regCenter, jobName);

//        jobEventConfig.createJobEventListener();

//        JobCoreConfiguration coreConfig = JobCoreConfiguration.newBuilder(jobName, "0/5 * * * * ?", 3)
//                .shardingItemParameters("0=Beijing,1=Shanghai,2=Guangzhou").build();

//        JobCoreConfiguration coreConfig = JobCoreConfiguration.newBuilder(jobName).build();

//        BizJobDefinition bizJobConfig = new BizJobDefinition(jobName, JavaSimpleJob.class
//                .getCanonicalName());
//
//        JobConfiguration jobConfig = JobConfiguration.newBuilder(bizJobConfig).build();

//        JobScheduleController JobScheduleController = new JobScheduleController(null, null, null);
//        JobRegistry.getInstance().registerJob(jobName, JobScheduleController, regCenter);


//        regCenter.addCacheData("/" + jobName);
//
//        JobInstance jobInstance = new JobInstance();
//        JobRegistry.getInstance().addJobInstance(jobConfig.getJobName(), jobInstance);
//
//        configService.persist(jobConfig);
//        JobConfiguration liteJobConfigFromRegCenter = configService.load(false);

//        electionListenerManager.start();
//        jobNodeStorage.addConnectionStateListener(regCenterConnectionStateListener);

//        shardingService.setReshardingFlag();


//        LiteJob liteJob = new LiteJob();
//        result.getJobDataMap().put("jobFacade", jobFacade);
//        result.getJobDataMap().put("elasticJob", Class.forName(jobClass).newInstance());

//        final LiteJobFacade jobFacade = new LiteJobFacade(regCenter, jobConfig.getJobName(), Arrays
//                .<ElasticJobListener>asList(new JavaSimpleListener()), new JobEventBus(jobEventConfig));
//
//        final JavaSimpleJob javaSimpleJob = new JavaSimpleJob();
//        Thread t = new Thread() {
//            public void run() {


//        JobExecutorFactory.getJobExecutor(javaSimpleJob, jobFacade).execute();


//            }
//        };
//        t.start();


//        new JobScheduler(regCenter, JobConfiguration.newBuilder(bizJobConfig).build(), jobEventConfig, new
//                JavaSimpleListener(), new JavaSimpleDistributeListener(1000L, 2000L)).init();