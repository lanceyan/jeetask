package com.jeeframework.jeetask.zookeeper.listener;


import com.jeeframework.jeetask.util.net.IPUtils;
import com.jeeframework.jeetask.zookeeper.instance.InstanceService;
import com.jeeframework.jeetask.zookeeper.server.ServerService;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;

/**
 * 注册中心连接状态监听器.
 *
 * @author lance
 */
public final class RegistryCenterConnectionStateListener implements ConnectionStateListener {


    private ServerService serverService;

    private InstanceService instanceService;

//    private final ShardingService shardingService;

//    private final ExecutionService executionService;

    public RegistryCenterConnectionStateListener(final ServerService
                                                         serverService) {
        this.serverService = serverService;
        this.instanceService = serverService.getInstanceService();
//        shardingService = new ShardingService(regCenter, jobName);
//        executionService = new ExecutionService(regCenter, jobName);
    }

    @Override
    public void stateChanged(final CuratorFramework client, final ConnectionState newState) {
//        if (JobRegistry.getInstance().isShutdown(jobName)) {
//            return;
//        }
//        JobScheduleController jobScheduleController = JobRegistry.getInstance().getJobScheduleController(jobName);
//        if (ConnectionState.SUSPENDED == newState || ConnectionState.LOST == newState) {
//            jobScheduleController.pauseJob();
//        } else
        if (ConnectionState.RECONNECTED == newState) {
            serverService.updateStatus(serverService.isEnableServer(IPUtils.getUniqueServerId()));
            instanceService.connect();
//            executionService.clearRunningInfo(shardingService.getLocalShardingItems());
//            jobScheduleController.resumeJob();
        }
    }
}
