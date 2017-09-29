/*
 * @project: jeetask 
 * @package: com.jeeframework.jeetask.server
 * @title:   Server.java 
 *
 * Copyright (c) 2017 jeeframework Limited, Inc.
 * All rights reserved.
 */
package com.jeeframework.jeetask.server;

import lombok.Getter;
import lombok.Setter;

/**
 * 服务器市实例类
 *
 * @author lance
 * @version 1.0 2017-08-16 17:27
 */
@Getter
@Setter
public class Server {
    private String outAndLocalIp;  //外网IP+内网IP
    private String outIp; //外网IP
    private String localIp;//内网IP
    private int taskCount;//任务数
    private int cpuCount;//cpu数量
    private int maxAllowedWorkerCount;//最大允许的线程数量

}
