package com.jeeframework.jeetask.zookeeper.listener;


import com.dangdang.ddframe.job.lite.internal.schedule.JobRegistry;
import com.dangdang.ddframe.job.lite.internal.schedule.JobScheduleController;
import com.dangdang.ddframe.job.reg.base.CoordinatorRegistryCenter;
import com.jeeframework.jeetask.zookeeper.instance.InstanceService;
import com.jeeframework.jeetask.zookeeper.server.ServerService;
import com.jeeframework.jeetask.zookeeper.sharding.ExecutionService;
import com.jeeframework.jeetask.zookeeper.sharding.ShardingService;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;

/**
 * 注册中心连接状态监听器.
 *
 * @author zhangliang
 */
public final class RegistryCenterConnectionStateListener implements ConnectionStateListener {

    private final String jobName;

    private ServerService serverService;

    private InstanceService instanceService;

    private final ShardingService shardingService;

    private final ExecutionService executionService;

    public RegistryCenterConnectionStateListener(final CoordinatorRegistryCenter regCenter, final String jobName) {
        this.jobName = jobName;
//        serverService = new ServerService(regCenter);
//        instanceService = new InstanceService(regCenter, jobName);
        shardingService = new ShardingService(regCenter, jobName);
        executionService = new ExecutionService(regCenter, jobName);
    }

    @Override
    public void stateChanged(final CuratorFramework client, final ConnectionState newState) {
        if (JobRegistry.getInstance().isShutdown(jobName)) {
            return;
        }
        JobScheduleController jobScheduleController = JobRegistry.getInstance().getJobScheduleController(jobName);
        if (ConnectionState.SUSPENDED == newState || ConnectionState.LOST == newState) {
            jobScheduleController.pauseJob();
        } else if (ConnectionState.RECONNECTED == newState) {
            serverService.updateStatus(serverService.isEnableServer(JobRegistry.getInstance().getJobInstance
                    (jobName).getIp()));
            instanceService.connect();
            executionService.clearRunningInfo(shardingService.getLocalShardingItems());
            jobScheduleController.resumeJob();
        }
    }
}
