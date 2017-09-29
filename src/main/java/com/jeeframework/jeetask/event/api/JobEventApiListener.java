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

package com.jeeframework.jeetask.event.api;

import com.jeeframework.jeetask.event.JobEventListener;
import com.jeeframework.jeetask.event.JobEventListenerConfigurationException;
import com.jeeframework.jeetask.event.JobEventProcessor;
import com.jeeframework.jeetask.event.api.impl.JobEvenApiProcessor;
import com.jeeframework.jeetask.event.type.JobExecutionEvent;
import com.jeeframework.util.classes.ClassUtils;
import com.jeeframework.util.validate.Validate;
import lombok.RequiredArgsConstructor;

/**
 * 运行痕迹事件Api监听器.
 *
 * @author lance
 */
@RequiredArgsConstructor
public final class JobEventApiListener implements JobEventListener {

    private final JobEventProcessor processor;

    public JobEventApiListener(final String jobEventStorageClass) throws JobEventListenerConfigurationException {
        if (!Validate.isEmpty(jobEventStorageClass)) {
            try {
                Class jobEventStorageClazz = ClassUtils.forName(jobEventStorageClass);
//                Constructor constructor = ClassUtils.getConstructorIfAvailable(jobEventStorageClazz, new
//                        Class[]{DataSource.class});
                processor = (JobEventProcessor) jobEventStorageClazz.newInstance();
            } catch (ClassNotFoundException e) {
                throw new JobEventListenerConfigurationException(e);
            } catch (IllegalAccessException e) {
                throw new JobEventListenerConfigurationException(e);
            } catch (InstantiationException e) {
                throw new JobEventListenerConfigurationException(e);
            }

        } else {
            processor = new JobEvenApiProcessor();
        }
    }

    @Override
    public void trigger(final JobExecutionEvent executionEvent) {
        processor.addJobExecutionEvent(executionEvent);
    }


//    @Override
//    public void listen(final JobStatusTraceEvent jobStatusTraceEvent) {
//        repository.addJobStatusTraceEvent(jobStatusTraceEvent);
//    }
}
