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

import com.jeeframework.jeetask.event.api.JobEventApiListener;
import com.jeeframework.jeetask.event.rdb.JobEventRdbListener;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import javax.sql.DataSource;
import java.io.Serializable;
import java.sql.SQLException;

/**
 * 作业数据库事件配置.
 *
 * @author lance
 */
@RequiredArgsConstructor
@Getter
public final class JobEventProcessorConfiguration implements JobEventConfiguration, Serializable {

    public static final String PROCESSOR_TYPE_DB = "db";
    public static final String PROCESSOR_TYPE_API = "api";

    private static final long serialVersionUID = 3344410699286435226L;

    private final String processorType;
    private final DataSource dataSource;
    private final String jobEventProcessor;

    @Override
    public JobEventListener createJobEventListener() throws JobEventListenerConfigurationException {
        try {
            if (PROCESSOR_TYPE_API.equals(processorType)) {
                return new JobEventApiListener(jobEventProcessor);
            } else {
                return new JobEventRdbListener(dataSource, jobEventProcessor);
            }
        } catch (final SQLException ex) {
            throw new JobEventListenerConfigurationException(ex);
        }
    }
}
