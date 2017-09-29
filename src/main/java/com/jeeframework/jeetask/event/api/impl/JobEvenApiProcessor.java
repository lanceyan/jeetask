/*
 * @project: jeetask 
 * @package: com.jeeframework.jeetask.event
 * @title:   JobEventProcessor.java 
 *
 * Copyright (c) 2017 jeeframework Limited, Inc.
 * All rights reserved.
 */
package com.jeeframework.jeetask.event.api.impl;

import com.jeeframework.jeetask.event.JobEventProcessor;
import com.jeeframework.jeetask.event.type.JobExecutionEvent;
import lombok.extern.slf4j.Slf4j;

/**
 * 作业事务API处理器
 *
 * @author lance
 * @version 1.0 2017-09-12 17:46
 */
@Slf4j
public class JobEvenApiProcessor extends JobEventProcessor {


    protected boolean insertJobExecutionEvent(final JobExecutionEvent jobExecutionEvent) {

        log.debug("JobEvenApiProcessor   insertJobExecutionEvent ");
        return true;
    }

    protected boolean updateJobExecutionEventWhenStaging(final JobExecutionEvent jobExecutionEvent) {

        log.debug("JobEvenApiProcessor   updateJobExecutionEventWhenStaging ");
        return true;
    }

    protected boolean updateJobExecutionEventWhenStart(final JobExecutionEvent jobExecutionEvent) {
        log.debug("JobEvenApiProcessor   updateJobExecutionEventWhenStart ");
        return true;
    }

    protected boolean updateJobExecutionEventWhenFinished(final JobExecutionEvent jobExecutionEvent) {
        log.debug("JobEvenApiProcessor   updateJobExecutionEventWhenFinished ");
        return true;
    }

    protected boolean updateJobExecutionEventFailure(final JobExecutionEvent jobExecutionEvent) {
        log.debug("JobEvenApiProcessor   updateJobExecutionEventFailure ");
        return true;
    }

    @Override
    protected boolean updateJobExecutionEventStopped(JobExecutionEvent jobExecutionEvent) {
        log.debug("JobEvenApiProcessor   updateJobExecutionEventStopped ");
        return false;
    }

    @Override
    protected boolean updateJobExecutionEventRecovery(JobExecutionEvent jobExecutionEvent) {
        log.debug("JobEvenApiProcessor   updateJobExecutionEventRecovery ");
        return false;
    }
}
