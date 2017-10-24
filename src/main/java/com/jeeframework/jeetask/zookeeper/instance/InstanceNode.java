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

package com.jeeframework.jeetask.zookeeper.instance;

import com.jeeframework.jeetask.util.net.IPUtils;
import com.jeeframework.jeetask.zookeeper.storage.NodePath;

/**
 * 运行实例节点路径.
 *
 * @author zhangliang
 */
public final class InstanceNode {
    private static final String DELIMITER = "@-@";
    /**
     * 运行实例信息根节点.
     */
    public static final String ROOT = "instances";

    private static final String INSTANCES = ROOT + "/%s";

    private final NodePath nodePath;

    private final static String instanceId;

    static {
        instanceId = IPUtils.getUniqueServerId() ;
//        + DELIMITER + DateFormatUtils
//                .format(new Date(), "yyyyMMddHHmmss")
    }

    public InstanceNode() {
        nodePath = new NodePath();
    }

    String getInstanceNode(final String instanceId) {
        return String.format(INSTANCES, instanceId);
    }

    /**
     * 判断给定路径是否为作业运行实例路径.
     *
     * @param path 待判断的路径
     * @return 是否为作业运行实例路径
     */
    public boolean isInstancePath(final String path) {
        return path.startsWith(nodePath.getFullPath(InstanceNode.ROOT));
    }

    boolean isLocalInstancePath(final String path) {
        return path.equals(nodePath.getFullPath(String.format(INSTANCES, getLocalInstanceId())));
    }

    String getLocalInstanceNode() {
        return String.format(INSTANCES, getLocalInstanceId());
    }

    public String getLocalInstanceId() {
        return instanceId;
    }

}
