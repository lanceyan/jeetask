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

package com.jeeframework.jeetask.executor;

import com.dangdang.ddframe.job.api.ElasticJob;
import com.dangdang.ddframe.job.api.simple.SimpleJob;
import com.dangdang.ddframe.job.exception.JobConfigurationException;
import com.jeeframework.jeetask.executor.type.SimpleJobExecutor;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/**
 * 作业执行器工厂.
 *
 * @author zhangliang
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class JobExecutorFactory {
    
    /**
     * 获取作业执行器.
     *
     * @param elasticJob 分布式弹性作业
     * @param jobFacade 作业内部服务门面服务
     * @return 作业执行器
     */
    @SuppressWarnings("unchecked")
    public static AbstractElasticJobExecutor getJobExecutor(final ElasticJob elasticJob, final JobFacade jobFacade) {

        if (elasticJob instanceof SimpleJob) {
            return new SimpleJobExecutor((SimpleJob) elasticJob, jobFacade);
        }

        throw new JobConfigurationException("Cannot support job type '%s'", elasticJob.getClass().getCanonicalName());
    }
}
