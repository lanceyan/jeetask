/*
 * @project: jeetask 
 * @package: com.jeeframework.jeetask.event
 * @title:   JobEventProcessor.java 
 *
 * Copyright (c) 2017 jeeframework Limited, Inc.
 * All rights reserved.
 */
package com.jeeframework.jeetask.event;

import com.jeeframework.jeetask.event.type.JobExecutionEvent;
import com.jeeframework.jeetask.event.type.JobStatusTraceEvent;

/**
 * 作业事务处理器
 *
 * @author lance
 * @version 1.0 2017-09-12 17:46
 */
public abstract class JobEventProcessor {
    public boolean addJobExecutionEvent(final JobExecutionEvent jobExecutionEvent) {

        JobStatusTraceEvent.State currentStat = jobExecutionEvent.getState();
        switch (currentStat) {
            case TASK_CREATED: {
                return insertJobExecutionEvent(jobExecutionEvent);
            }
            case TASK_STAGING: {
                return updateJobExecutionEventWhenStaging(jobExecutionEvent);
            }
            case TASK_RUNNING: {
                return updateJobExecutionEventWhenStart(jobExecutionEvent);
            }
            case TASK_FINISHED: {
                return updateJobExecutionEventWhenFinished(jobExecutionEvent);
            }
            case TASK_ERROR: {
                return updateJobExecutionEventFailure(jobExecutionEvent);
            }
            case TASK_STOPPED: {
                return updateJobExecutionEventStopped(jobExecutionEvent);
            }
            case TASK_RECOVERY: {
                return updateJobExecutionEventRecovery(jobExecutionEvent);
            }

        }

        return true;
    }

    protected abstract boolean insertJobExecutionEvent(final JobExecutionEvent jobExecutionEvent);

    protected abstract boolean updateJobExecutionEventWhenStaging(final JobExecutionEvent jobExecutionEvent);

    protected abstract boolean updateJobExecutionEventWhenStart(final JobExecutionEvent jobExecutionEvent);

    protected abstract boolean updateJobExecutionEventWhenFinished(final JobExecutionEvent jobExecutionEvent);

    protected abstract boolean updateJobExecutionEventFailure(final JobExecutionEvent jobExecutionEvent);

    protected abstract boolean updateJobExecutionEventStopped(final JobExecutionEvent jobExecutionEvent);

    protected abstract boolean updateJobExecutionEventRecovery(final JobExecutionEvent jobExecutionEvent);
}
