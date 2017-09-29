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

package com.jeeframework.jeetask.zookeeper.election;

import com.jeeframework.jeetask.zookeeper.storage.NodePath;

/**
 * 主节点路径.
 *
 * @author zhangliang
 */
public final class LeaderNode {

    /**
     * 主节点根路径.
     */
    public static final String ROOT = "leader";

    static final String ELECTION_ROOT = ROOT + "/election";

    static final String INSTANCE = ELECTION_ROOT + "/instance";

    static final String LATCH = ELECTION_ROOT + "/latch";

    private final NodePath nodePath;

    public LeaderNode() {
        nodePath = new NodePath();
    }

    public boolean isLeaderInstancePath(final String path) {
        return nodePath.getFullPath(INSTANCE).equals(path);
    }
}
