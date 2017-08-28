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

import com.dangdang.ddframe.job.util.env.IpUtils;
import com.jeeframework.jeetask.zookeeper.storage.NodePath;

import java.util.regex.Pattern;

/**
 * 服务器节点路径.
 *
 * @author zhangliang
 */
public final class ServerNode {

    /**
     * 服务器信息根节点.
     */
    public static final String ROOT = "servers";

    private static final String SERVERS = ROOT + "/%s";

    private static final String TASK_COUNT = SERVERS + "/taskCount";
    private static final String TASK_NODE = SERVERS + "/tasks";

    private static final String TASK_WAITING_NODE = TASK_NODE + "/waiting";
    private static final String TASK_WAITING_NODE_ID = TASK_WAITING_NODE + "/%s";  //等待运行节点

    private static final String TASK_RUNNING_NODEE = TASK_NODE + "/running";
    private static final String TASK_RUNNING_NODEE_ID = TASK_RUNNING_NODEE + "/%s";  //运行节点


    private final NodePath nodePath;

    public ServerNode() {
        nodePath = new NodePath();
    }

    /**
     * 判断给定路径是否为作业服务器路径.
     *
     * @param path 待判断的路径
     * @return 是否为作业服务器路径
     */
    public boolean isServerPath(final String path) {
        return Pattern.compile(nodePath.getFullPath(ServerNode.ROOT) + "/" + IpUtils.IP_REGEX).matcher(path)
                .matches();
    }
//
//    /**
//     * 判断给定路径是否为本地作业服务器路径.
//     *
//     * @param path 待判断的路径
//     * @return 是否为本地作业服务器路径
//     */
//    public boolean isLocalServerPath(final String path) {
//        return path.equals(nodePath.getFullPath(String.format(SERVERS, JobRegistry.getInstance().getJobInstance
//                (jobName).getIp())));
//    }

    public static String getServerNode(final String ip) {
        return String.format(SERVERS, ip);
    }

    public static String getTaskCountNode(final String ip) {
        return String.format(TASK_COUNT, ip);
    }

    public static String getTaskNode(final String ip) {
        return String.format(TASK_NODE, ip);
    }

    public static String getWaitingTaskNode(final String ip) {
        return String.format(TASK_WAITING_NODE, ip);
    }

    public static String getRunningTaskNode(final String ip) {
        return String.format(TASK_RUNNING_NODEE, ip);
    }

    public static String getRunningTaskIdNode(final String ip, final long taskId) {
        return String.format(TASK_RUNNING_NODEE_ID, ip, taskId);
    }

    public static String getWaitingTaskIdNode(final String ip, final long taskId) {
        return String.format(TASK_WAITING_NODE_ID, ip, taskId);
    }
}
