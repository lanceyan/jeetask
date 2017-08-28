/*
 * @project: jeetask 
 * @package: com.jeeframework.jeetask.zookeeper.embed
 * @title:   EmbedZookeeperServer.java 
 *
 * Copyright (c) 2017 jeeframework Limited, Inc.
 * All rights reserved.
 */
package com.jeeframework.jeetask.zookeeper.embed;

import org.apache.curator.test.TestingServer;

import java.io.File;
import java.io.IOException;

/**
 * 内嵌zookeeper server用于测试时使用，在生产环境建议还是使用standalone模式的部署结构
 *
 * @author lance
 * @version 1.0 2017-04-26 15:53
 */
public class EmbedZookeeperServer {
    private static TestingServer testingServer;

    /**
     * 内存版的内嵌Zookeeper.
     *
     * @param port Zookeeper的通信端口号
     */
    public static void start(final int port) {
        try {
            testingServer = new TestingServer(port, new File(String.format("build/test_zk_data/%s/", System.nanoTime
                    ())));
            // CHECKSTYLE:OFF
        } catch (final Exception ex) {
            // CHECKSTYLE:ON
            ex.printStackTrace();
        } finally {
            Runtime.getRuntime().addShutdownHook(new Thread() {

                @Override
                public void run() {
                    try {
                        Thread.sleep(1000L);
                        testingServer.close();
                    } catch (final InterruptedException | IOException ex) {
                        ex.printStackTrace();
                    }
                }
            });
        }
    }

}
