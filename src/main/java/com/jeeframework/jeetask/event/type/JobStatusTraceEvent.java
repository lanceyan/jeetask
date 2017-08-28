package com.jeeframework.jeetask.event.type;

import com.jeeframework.jeetask.event.JobEvent;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;

import java.util.Date;

/**
 * 作业状态痕迹事件.
 *
 * @author caohao
 */
@RequiredArgsConstructor
@AllArgsConstructor
@Getter
public final class JobStatusTraceEvent implements JobEvent {

    @Setter
    private String taskName;

//    @Setter
//    private String originalTaskId = "";

    private final String taskId;

//    private final ExecutionType executionType;

    private final State state;

    private final String message;

    private Date creationTime = new Date();

    //TASK_CREATED, TASK_STAGING, TASK_RUNNING, TASK_FINISHED, TASK_KILLED, TASK_LOST, TASK_ERROR
    public enum State {
        TASK_CREATED, TASK_STAGING, TASK_RUNNING, TASK_FINISHED, TASK_KILLED, TASK_LOST, TASK_ERROR
    }


}
