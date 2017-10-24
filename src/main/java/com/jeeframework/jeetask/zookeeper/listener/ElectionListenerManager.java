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

package com.jeeframework.jeetask.zookeeper.listener;


import com.dangdang.ddframe.job.reg.base.CoordinatorRegistryCenter;
import com.jeeframework.jeetask.util.net.IPUtils;
import com.jeeframework.jeetask.zookeeper.election.LeaderNode;
import com.jeeframework.jeetask.zookeeper.election.LeaderService;
import com.jeeframework.jeetask.zookeeper.server.ServerNode;
import com.jeeframework.jeetask.zookeeper.server.ServerService;
import com.jeeframework.jeetask.zookeeper.storage.NodePath;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent.Type;

/**
 * 主节点选举监听管理器.
 *
 * @author zhangliang
 */
public final class ElectionListenerManager extends AbstractListenerManager {

    private final LeaderNode leaderNode;

    private final ServerNode serverNode;

    private final LeaderService leaderService;

    private final ServerService serverService;

    private final NodePath nodePath;

    public ElectionListenerManager(final CoordinatorRegistryCenter regCenter, final
    LeaderService leaderService, final ServerService serverService) {
        super(regCenter);
        leaderNode = new LeaderNode();
        serverNode = new ServerNode();
        this.leaderService = leaderService;
        this.serverService = serverService;
        this.nodePath = new NodePath();
        regCenter.addCacheData(nodePath.getFullPath(LeaderNode.ROOT));
    }

    @Override
    public void start() {
        addDataListener(nodePath.getFullPath(LeaderNode.ROOT), new LeaderElectionJobListener());
        addDataListener(nodePath.getFullPath(LeaderNode.ROOT), new LeaderAbdicationJobListener());
    }

    class LeaderElectionJobListener extends AbstractJobListener {

        @Override
        protected void dataChanged(final String path, final Type eventType, final String data) {
            //isActiveElection(path, data) ||
            if ((isPassiveElection
                    (path, eventType))) {
                leaderService.electLeader();
            }
        }

//        private boolean isActiveElection(final String path, final String data) {
//            return !leaderService.hasLeader() && isLocalServerEnabled(path, data);
//        }

        private boolean isPassiveElection(final String path, final Type eventType) {
            return isLeaderCrashed(path, eventType) && serverService.isAvailableServer(IPUtils.getUniqueServerId());
        }

        private boolean isLeaderCrashed(final String path, final Type eventType) {
            return leaderNode.isLeaderInstancePath(path) && Type.NODE_REMOVED == eventType;
        }

//        private boolean isLocalServerEnabled(final String path, final String data) {
//            return serverNode.isLocalServerPath(path) && !ServerStatus.DISABLED.name().equals(data);
//        }
    }

    class LeaderAbdicationJobListener extends AbstractJobListener {

        @Override
        protected void dataChanged(final String path, final Type eventType, final String data) {
            ;
            if (leaderService.isLeader() && !serverService.isEnableServer(IPUtils.getUniqueServerId())) {
                leaderService.removeLeader();
            }
        }

//        private boolean isLocalServerDisabled(final String path, final String data) {
//            return serverNode.isLocalServerPath(path) && ServerStatus.DISABLED.name().equals(data);
//        }
    }
}
