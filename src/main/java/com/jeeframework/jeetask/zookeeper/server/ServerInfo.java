/*
 * @project: jeetask 
 * @package: com.jeeframework.jeetask.zookeeper.server
 * @title:   ServerInfo.java 
 *
 * Copyright (c) 2017 jeeframework Limited, Inc.
 * All rights reserved.
 */
package com.jeeframework.jeetask.zookeeper.server;

/**
 * 服务器信息
 *
 * @author lance
 * @version 1.0 2017-08-02 13:46
 */
public class ServerInfo {
    private int cpu;  //cpu数量
    private String status; //状态  enabled  disabled

    public int getCpu() {
        return cpu;
    }

    public void setCpu(int cpu) {
        this.cpu = cpu;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }
}
