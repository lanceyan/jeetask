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
import com.jeeframework.jeetask.event.rdb.JobEventRdbConfiguration;
import com.jeeframework.jeetask.util.resource.JeeTaskPropertiesUtil;
import com.jeeframework.jeetask.zookeeper.embed.EmbedZookeeperServer;
import com.jeeframework.jeetask.zookeeper.reg.ZookeeperConfiguration;
import com.jeeframework.jeetask.zookeeper.reg.ZookeeperRegistryCenter;
import com.jeeframework.util.validate.Validate;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.dbcp2.BasicDataSource;

import javax.sql.DataSource;
import java.io.IOException;
import java.util.Properties;

/**
 * jeetask的服务基类
 *
 * @author lance
 * @version 1.0 2017-08-28 17:43
 */
@Slf4j
public class JeeTask {
    private static final int EMBED_ZOOKEEPER_PORT = 2181;
    private static final String ZOOKEEPER_CONNECTION_STRING = "localhost:" + EMBED_ZOOKEEPER_PORT;
    private static final String JOB_NAMESPACE = "jeetask";

    protected final Properties properties = new Properties();
    private DataSource dataSource;

    protected CoordinatorRegistryCenter regCenter;
    protected JobEventBus jobEventBus;

    public JeeTask(DataSource taskDataSource) throws IOException {
        properties.putAll(JeeTaskPropertiesUtil.getProperties());
        if (null == taskDataSource) {
            this.dataSource = setUpEventTraceDataSource();
        }

        regCenter = setUpRegistryCenter();
        //String dbType = properties.getProperty("db.type");
        String jobEventStorageClass = properties.getProperty("db.jobEventStorageClass");
        if (Validate.isEmpty(jobEventStorageClass)) {
            jobEventStorageClass = "";//为空默认为
        }
        JobEventRdbConfiguration jobEventConfig = new JobEventRdbConfiguration(this.dataSource, jobEventStorageClass);
        jobEventBus = new JobEventBus(jobEventConfig);
    }

    protected CoordinatorRegistryCenter setUpRegistryCenter() {
//        #zookeeper.connection.string=localhost:2181
//        #zookeeper.embed.port=2181
//        zookeeper.job.namespace=jeetask


        String zookeeperConnectionString = properties.getProperty("zookeeper.connection.string");
        String zookeeperEmbedPort = properties.getProperty("zookeeper.embed.port");
        String zookeeperJobNamespace = properties.getProperty("zookeeper.job.namespace");

        if (Validate.isEmpty(zookeeperConnectionString)) {
            if (Validate.isEmpty(zookeeperEmbedPort)) {
                zookeeperConnectionString = ZOOKEEPER_CONNECTION_STRING;
                if (this instanceof JeeTaskServer) {
                    EmbedZookeeperServer.start(EMBED_ZOOKEEPER_PORT);
                }
            } else {
                zookeeperConnectionString = "localhost:" + zookeeperEmbedPort;
                int embedPort = Integer.valueOf(zookeeperEmbedPort);
                if (this instanceof JeeTaskServer) {
                    EmbedZookeeperServer.start(embedPort);
                }
            }
            log.info("没有检测到zookeeper连接配置，使用默认连接标识为：" + zookeeperConnectionString);

        }
        if (Validate.isEmpty(zookeeperJobNamespace)) {
            zookeeperJobNamespace = JOB_NAMESPACE;
        }

        ZookeeperConfiguration zkConfig = new ZookeeperConfiguration(zookeeperConnectionString, zookeeperJobNamespace);
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
}
