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

package com.jeeframework.jeetask.event;

import com.dangdang.ddframe.job.util.concurrent.ExecutorServiceObject;
import com.google.common.eventbus.AsyncEventBus;
import com.google.common.eventbus.EventBus;
import com.jeeframework.jeetask.event.type.JobExecutionEvent;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * 运行痕迹事件总线.
 *
 * @author zhangliang
 * @author caohao
 */
@Slf4j
public final class JobEventBus {

    private final JobEventConfiguration jobEventConfig;

    private final ExecutorServiceObject executorServiceObject;

    private final EventBus eventBus;

    private boolean isRegistered;
    @Getter
    private JobEventListener jobEventListener;

    public JobEventBus() {
        jobEventConfig = null;
        executorServiceObject = null;
        eventBus = null;
    }

    public JobEventBus(final JobEventConfiguration jobEventConfig) {
        this.jobEventConfig = jobEventConfig;
        executorServiceObject = new ExecutorServiceObject("job-event", Runtime.getRuntime().availableProcessors() * 2);
        eventBus = new AsyncEventBus(executorServiceObject.createExecutorService());
        try {
            jobEventListener = jobEventConfig.createJobEventListener();
        } catch (final JobEventListenerConfigurationException ex) {
            log.error("Elastic job: create JobEventListener failure, error is: ", ex);
        }
        register();
    }

    private void register() {
        eventBus.register(jobEventListener);
        isRegistered = true;

    }

    /**
     * 发布事件.
     *
     * @param event 作业事件
     */
    public void post(final JobEvent event) {
        if (isRegistered && !executorServiceObject.isShutdown()) {
            eventBus.post(event);
        }
    }

    public void trigger(final JobExecutionEvent event) {
        jobEventListener.trigger(event);
    }
}
