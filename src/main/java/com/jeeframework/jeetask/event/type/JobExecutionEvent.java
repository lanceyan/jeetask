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

package com.jeeframework.jeetask.event.type;

import com.dangdang.ddframe.job.exception.ExceptionUtil;
import com.jeeframework.jeetask.event.JobEvent;
import com.jeeframework.jeetask.task.Task;
import com.jeeframework.jeetask.util.net.IPUtils;
import lombok.Getter;
import lombok.Setter;

import java.util.Date;

/**
 * 作业执行事件.
 *
 * @author zhangliang
 */
@Getter
public final class JobExecutionEvent implements JobEvent {

//    private String id = UUID.randomUUID().toString();

//    private String hostname = IpUtils.getHostName();

//    id	bigint	20	0	0	-1	0	0	0		0					-1	0
//    name	varchar	100	0	0	0	0	0	0		0	任务名	utf8	utf8_general_ci		0	0
//    state	int	5	0	0	0	0	0	0		0	状态 0  待分配   1 待启动  2 运行中   3  完成  9 出错				0	0
//    progress	int	5	0	0	0	0	0	0		0	进度				0	0
//    ip	varchar	50	0	0	0	0	0	0		0	任务被分配到的IP	utf8	utf8_general_ci		0	0
//    param	varchar	1000	0	0	0	0	0	0		0	提交的任务参数，时间范围，条数等等	utf8	utf8_general_ci		0	0
//    message	varchar	511	0	0	0	0	0	0		0	错误消息	utf8	utf8_general_ci		0	0
//    create_time	timestamp	0	0	-1	0	0	0	0		0	创建时间				0	0
//    start_time	timestamp	0	0	-1	0	0	0	0		0	开始时间				0	0
//    complete_time	timestamp	0	0	-1	0	0	0	0		0	结束时间				0	0
//    failure_cause	varchar	4000	0	-1	0	0	0	0		0	失败堆栈	utf8	utf8_general_ci		0	0


    @Setter
    private long taskId;
    @Setter
    private String taskName;

    private final JobStatusTraceEvent.State state;
    @Setter
    private int progress;
    private final String ip;
    @Setter
    private String param;
    @Setter
    private String message;

    private Date createTime = new Date();

    @Setter
    private Date startTime;

    @Setter
    private Date completeTime;

    @Setter
    private JobExecutionEventThrowable failureCause;

    public JobExecutionEvent(long taskId, JobStatusTraceEvent.State state, String ip) {
        this.taskId = taskId;
        this.state = state;
        this.ip = ip;
    }

    public JobExecutionEvent(Task task) {
        this.taskId = taskId;
        this.taskName = task.getName() == null ? "" : task.getName();
        this.param = task.getParam() == null ? "" : task.getParam();
        this.message = "";
        this.state = JobStatusTraceEvent.State.TASK_CREATED;
        this.ip = "";
    }


    /**
     * 作业执行成功.
     *
     * @return 作业执行事件
     */
    public JobExecutionEvent executionSuccess() {
        JobExecutionEvent result = new JobExecutionEvent(taskId, state.TASK_FINISHED, IPUtils.getOutAndLocalIPV4());
        result.setCompleteTime(new Date());
        return result;
    }

    /**
     * 作业执行失败.
     *
     * @param failureCause 失败原因
     * @return 作业执行事件
     */
    public JobExecutionEvent executionFailure(final Throwable failureCause) {
        JobExecutionEvent result = new JobExecutionEvent(taskId, state.TASK_ERROR, IPUtils.getOutAndLocalIPV4());

        result.setFailureCause(new JobExecutionEventThrowable
                (failureCause));
        result.setCompleteTime(new Date());
        return result;
    }

    /**
     * 获取失败原因.
     *
     * @return 失败原因
     */
    public String getFailureCause() {
        return ExceptionUtil.transform(failureCause == null ? null : failureCause.getThrowable());
    }


}
