/*
 * @project: jeetask 
 * @package: com.jeeframework.jeetask.startup
 * @title:   JeeTask.java 
 *
 * Copyright (c) 2017 jeeframework Limited, Inc.
 * All rights reserved.
 */
package com.jeeframework.jeetask.startup;

import com.dangdang.ddframe.job.reg.base.CoordinatorRegistryCenter;
import com.jeeframework.jeetask.event.JobEventBus;
import com.jeeframework.jeetask.event.JobEventProcessorConfiguration;
import com.jeeframework.jeetask.util.resource.JeeTaskPropertiesUtil;
import com.jeeframework.jeetask.zookeeper.embed.EmbedZookeeperServer;
import com.jeeframework.jeetask.zookeeper.reg.ZookeeperConfiguration;
import com.jeeframework.jeetask.zookeeper.reg.ZookeeperRegistryCenter;
import com.jeeframework.util.validate.Validate;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.dbcp2.BasicDataSource;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.InitializingBean;

import javax.sql.DataSource;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

/**
 * jeetask的服务基类
 *
 * @author lance
 * @version 1.0 2017-08-28 17:43
 */
@Slf4j
public class JeeTask implements BeanFactoryAware, InitializingBean {
    private static final int EMBED_ZOOKEEPER_PORT = 2181;
    private static final String ZOOKEEPER_CONNECTION_STRING = "localhost:" + EMBED_ZOOKEEPER_PORT;
    private static final String JOB_NAMESPACE = "jeetask";

    protected final Properties properties = new Properties();
    @Setter
    private DataSource dataSource;
    @Setter
    private String zookeeperConnectionString;
    @Setter
    private String zookeeperEmbedPort;
    @Setter
    private String zookeeperJobNamespace;

    @Setter
    private String zookeeperUserName;
    @Setter
    private String zookeeperPassword;

    @Setter
    private String jobEventProcessorClass;
    @Setter
    private String jobEventProcessorType;


    protected CoordinatorRegistryCenter regCenter;
    protected JobEventBus jobEventBus;

    protected BeanFactory context;

    public JeeTask() {
        try {
            properties.putAll(JeeTaskPropertiesUtil.getProperties());
        } catch (FileNotFoundException e) {
            log.debug("找不到文件，跳过构造器加载 ");
            return;
            //如果不是通过task.properties方式启动，就返回
        } catch (IOException e) {
            log.debug("读写文件错误，跳过构造器加载 ");
            return;
        }
        this.dataSource = null;

        init();
    }

    public JeeTask(DataSource taskDataSource) throws IOException {
        try {
            properties.putAll(JeeTaskPropertiesUtil.getProperties());
        } catch (FileNotFoundException e) {
            return;
            //如果不是通过task.properties方式启动，就返回
        }
        this.dataSource = taskDataSource;

        init();
    }

    private void init() {

        regCenter = setUpRegistryCenter();

//        NodeStorage nodeStorage = new NodeStorage(regCenter);
//
//        CuratorFramework client = nodeStorage.getClient();
//        NodePath nodePath = new NodePath();
//        DistributedAtomicInteger counter = new DistributedAtomicInteger(client, nodePath.getFullPath(ServerNode
//                .getTaskCountNode
//                        (IPUtils.getUniqueServerId())), new
//                RetryNTimes(100, 1000));
//        try {
//            counter.increment();
//        } catch (Exception e) {
//            e.printStackTrace();
//        }

        //String dbType = properties.getProperty("db.type");
        String jobEventProcessorClassTmp = properties.getProperty("task.jobEventProcessorClass");
        //properties里没有就判断通过spring方式注入有没有
        if (Validate.isEmpty(jobEventProcessorClassTmp)) {
            jobEventProcessorClassTmp = jobEventProcessorClass;
        }

        if (Validate.isEmpty(jobEventProcessorClassTmp)) {
            jobEventProcessorClassTmp = "";//为空默认为
        }

        String jobEventProcessorTypeTmp = properties.getProperty("task.jobEventProcessorType");
        if (Validate.isEmpty(jobEventProcessorTypeTmp)) {
            jobEventProcessorTypeTmp = jobEventProcessorType;
        }
        if (Validate.isEmpty(jobEventProcessorTypeTmp)) {
            jobEventProcessorTypeTmp = JobEventProcessorConfiguration.PROCESSOR_TYPE_DB;
        }
        log.debug("配置job事件采用 " + jobEventProcessorTypeTmp + "  响应");
        if (JobEventProcessorConfiguration.PROCESSOR_TYPE_DB.equals(jobEventProcessorTypeTmp)) {
            if (null == this.dataSource) {
                this.dataSource = setUpEventTraceDataSource();
            }
        }

        JobEventProcessorConfiguration jobEventConfig = new JobEventProcessorConfiguration(jobEventProcessorTypeTmp,
                this.dataSource,
                jobEventProcessorClassTmp);
        jobEventBus = new JobEventBus(jobEventConfig);
    }

    protected CoordinatorRegistryCenter setUpRegistryCenter() {
//        #zookeeper.connection.string=localhost:2181
//        #zookeeper.embed.port=2181
//        zookeeper.job.namespace=jeetask


        String zookeeperConnectionStringTmp = properties.getProperty("zookeeper.connection.string");
        String zookeeperEmbedPortTmp = properties.getProperty("zookeeper.embed.port");
        String zookeeperJobNamespaceTmp = properties.getProperty("zookeeper.job.namespace");

        String zookeeperUserNameTmp = properties.getProperty("zookeeper.username");
        String zookeeperPasswordTmp = properties.getProperty("zookeeper.password");

        if (Validate.isEmpty(zookeeperConnectionStringTmp)) {
            zookeeperConnectionStringTmp = zookeeperConnectionString;
        }
        if (Validate.isEmpty(zookeeperEmbedPortTmp)) {
            zookeeperEmbedPortTmp = zookeeperEmbedPort;
        }
        if (Validate.isEmpty(zookeeperJobNamespaceTmp)) {
            zookeeperJobNamespaceTmp = zookeeperJobNamespace;
        }

        if (Validate.isEmpty(zookeeperUserNameTmp)) {
            zookeeperUserNameTmp = zookeeperUserName;
        }
        if (Validate.isEmpty(zookeeperPasswordTmp)) {
            zookeeperPasswordTmp = zookeeperPassword;
        }

        int embedPort = -1;
        if (Validate.isEmpty(zookeeperConnectionStringTmp)) {
            if (Validate.isEmpty(zookeeperEmbedPortTmp)) {
                zookeeperConnectionStringTmp = ZOOKEEPER_CONNECTION_STRING;
                embedPort = EMBED_ZOOKEEPER_PORT;
            } else {
                zookeeperConnectionStringTmp = "localhost:" + zookeeperEmbedPortTmp;
                embedPort = Integer.valueOf(zookeeperEmbedPortTmp);
            }
            if (this instanceof JeeTaskServer) {
                if (embedPort < 0) {
                    embedPort = EMBED_ZOOKEEPER_PORT;
//                    throw new JeeTaskConfigurationException("没有zookeeper连接配置，检测到没有嵌入式端口配置，启动失败");
                }
                EmbedZookeeperServer.start(embedPort);
                log.info("没有检测到zookeeper连接配置，启动嵌入式zookeeper ：" + zookeeperConnectionStringTmp);
            } else {
                log.info("没有检测到zookeeper连接配置，使用默认连接标识为：" + zookeeperConnectionStringTmp);
            }

        }
        if (Validate.isEmpty(zookeeperJobNamespaceTmp)) {
            zookeeperJobNamespaceTmp = JOB_NAMESPACE;
        }

        ZookeeperConfiguration zkConfig = new ZookeeperConfiguration(zookeeperConnectionStringTmp,
                zookeeperJobNamespaceTmp);
        String digest = null;

        if (!Validate.isEmpty(zookeeperPasswordTmp) && !Validate.isEmpty(zookeeperUserNameTmp)) {
            digest = zookeeperUserNameTmp + ":" + zookeeperPasswordTmp;
            zkConfig.setDigest(digest);
        }

        CoordinatorRegistryCenter result = new ZookeeperRegistryCenter(zkConfig);
        result.init();
        return result;
    }

    protected DataSource setUpEventTraceDataSource() {
        String dbDriver = properties.getProperty("db.driver");
        String dbUrl = properties.getProperty("db.url");
        String dbUserName = properties.getProperty("db.username");
        String dbPassword = properties.getProperty("db.password");


        BasicDataSource result = new BasicDataSource();
        result.setDriverClassName(dbDriver);
        result.setUrl(dbUrl);
        result.setUsername(dbUserName);
        result.setPassword(dbPassword);
        return result;
    }

    @Override
    public void setBeanFactory(BeanFactory beanFactory) throws BeansException {
        this.context = beanFactory;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        init();
    }
}
