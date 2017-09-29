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

package com.jeeframework.jeetask.zookeeper.storage;

import com.dangdang.ddframe.job.exception.JobSystemException;
import com.dangdang.ddframe.job.reg.base.CoordinatorRegistryCenter;
import com.jeeframework.jeetask.zookeeper.exception.ZkOperationExceptionHandler;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.transaction.CuratorTransactionFinal;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.framework.recipes.cache.TreeCacheListener;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.state.ConnectionStateListener;

import java.util.List;

/**
 * 作业节点数据访问类.
 * <p>
 * <p>
 * 作业节点是在普通的节点前加上作业名称的前缀.
 * </p>
 *
 * @author zhangliang
 */
public final class NodeStorage {

    private final CoordinatorRegistryCenter regCenter;

    private final NodePath nodePath;

    public NodeStorage(final CoordinatorRegistryCenter regCenter) {
        this.regCenter = regCenter;
        nodePath = new NodePath();
    }

    /**
     * 判断作业节点是否存在.
     *
     * @param node 作业节点名称
     * @return 作业节点是否存在
     */
    public boolean isNodeExisted(final String node) {
        return regCenter.isExisted(nodePath.getFullPath(node));
    }

    /**
     * 获取作业节点数据.
     *
     * @param node 作业节点名称
     * @return 作业节点数据值
     */
    public String getNodeData(final String node) {
        return regCenter.get(nodePath.getFullPath(node));
    }

    /**
     * 直接从注册中心而非本地缓存获取作业节点数据.
     *
     * @param node 作业节点名称
     * @return 作业节点数据值
     */
    public String getNodeDataDirectly(final String node) {
        return regCenter.getDirectly(nodePath.getFullPath(node));
    }

    /**
     * 获取作业节点子节点名称列表.
     *
     * @param node 作业节点名称
     * @return 作业节点子节点名称列表
     */
    public List<String> getNodeChildrenKeys(final String node) {
        return regCenter.getChildrenKeys(nodePath.getFullPath(node));
    }

    /**
     * 如果存在则创建作业节点.
     * <p>
     * <p>如果作业根节点不存在表示作业已经停止, 不再继续创建节点.</p>
     *
     * @param node 作业节点名称
     */
    public void createNodeIfNeeded(final String node) {
        if (!isNodeExisted(node)) {
            regCenter.persist(nodePath.getFullPath(node), "");
        }
    }

    /**
     * 检查节点是否存在，删除作业节点.
     *
     * @param node 作业节点名称
     */
    public void removeNodeIfExisted(final String node) {
        if (isNodeExisted(node)) {
            regCenter.remove(nodePath.getFullPath(node));
        }
    }

    /**
     * 删除作业节点
     *
     * @param node
     */
    public void removeNode(final String node) {
        regCenter.remove(nodePath.getFullPath(node));
    }

    /**
     * 填充节点数据.
     *
     * @param node  作业节点名称
     * @param value 作业节点数据值
     */
    public void fillNode(final String node, final Object value) {
        regCenter.persist(nodePath.getFullPath(node), value.toString());
    }

    /**
     * 填充临时节点数据.
     *
     * @param node  作业节点名称
     * @param value 作业节点数据值
     */
    public void fillEphemeralNode(final String node, final Object value) {
        regCenter.persistEphemeral(nodePath.getFullPath(node), value.toString());
    }

    /**
     * 更新节点数据.
     *
     * @param node  作业节点名称
     * @param value 作业节点数据值
     */
    public void updateNode(final String node, final Object value) {
        regCenter.update(nodePath.getFullPath(node), value.toString());
    }

    /**
     * 替换作业节点数据.
     *
     * @param node  作业节点名称
     * @param value 待替换的数据
     */
    public void replaceNode(final String node, final Object value) {
        regCenter.persist(nodePath.getFullPath(node), value.toString());
    }

    /**
     * 在事务中执行操作.
     *
     * @param callback 执行操作的回调
     */
    public void executeInTransaction(final TransactionExecutionCallback callback) {
        try {
            CuratorTransactionFinal curatorTransactionFinal = getClient().inTransaction().check().forPath("/").and();
            callback.execute(curatorTransactionFinal);
            curatorTransactionFinal.commit();
            callback.afterCommit();
            //CHECKSTYLE:OFF
        } catch (final Exception ex) {
            //CHECKSTYLE:ON
            ZkOperationExceptionHandler.handleException(ex);
        }
    }

    /**
     * 在主节点执行操作.
     *
     * @param latchNode 分布式锁使用的作业节点名称
     * @param callback  执行操作的回调
     */
    public void executeInLeader(final String latchNode, final LeaderExecutionCallback callback) {
        try (LeaderLatch latch = new LeaderLatch(getClient(), nodePath.getFullPath(latchNode))) {
            latch.start();
            latch.await();
//            System.out.println(latch.getLeader());
            callback.execute();
            //CHECKSTYLE:OFF
        } catch (final Exception ex) {
            //CHECKSTYLE:ON
            handleException(ex);
        }
    }

    private void handleException(final Exception ex) {
        if (ex instanceof InterruptedException) {
            Thread.currentThread().interrupt();
        } else {
            throw new JobSystemException(ex);
        }
    }

    /**
     * 注册连接状态监听器.
     *
     * @param listener 连接状态监听器
     */
    public void addConnectionStateListener(final ConnectionStateListener listener) {
        getClient().getConnectionStateListenable().addListener(listener);
    }

    public CuratorFramework getClient() {
        return (CuratorFramework) regCenter.getRawClient();
    }

    /**
     * 注册数据监听器.
     *
     * @param path     监听的路径
     * @param listener 数据监听器
     */
    public void addDataListener(final String path, final TreeCacheListener listener) {
        TreeCache cache = (TreeCache) regCenter.getRawCache(path);
        cache.getListenable().addListener(listener);
    }

    /**
     * 获取注册中心当前时间.
     *
     * @return 注册中心当前时间
     */
    public long getRegistryCenterTime() {
        return regCenter.getRegistryCenterTime(nodePath.getFullPath("systemTime/current"));
    }
}
