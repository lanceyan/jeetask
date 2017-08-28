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

package com.jeeframework.jeetask.zookeeper.config;


import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import com.jeeframework.jeetask.config.JobDefinition;
import com.jeeframework.jeetask.config.simple.JobConfiguration;
import com.jeeframework.jeetask.executor.handler.JobProperties;
import com.jeeframework.jeetask.util.json.AbstractJobConfigurationGsonTypeAdapter;
import com.jeeframework.jeetask.util.json.GsonFactory;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import java.io.IOException;
import java.util.Map;

/**
 * Lite作业配置的Gson工厂.
 *
 * @author zhangliang
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class LiteJobConfigurationGsonFactory {

    static {
        GsonFactory.registerTypeAdapter(JobConfiguration.class, new LiteJobConfigurationGsonTypeAdapter());
    }

    /**
     * 将作业配置转换为JSON字符串.
     *
     * @param liteJobConfig 作业配置对象
     * @return 作业配置JSON字符串
     */
    public static String toJson(final JobConfiguration liteJobConfig) {
        return GsonFactory.getGson().toJson(liteJobConfig);
    }

    /**
     * 将作业配置转换为JSON字符串.
     *
     * @param liteJobConfig 作业配置对象
     * @return 作业配置JSON字符串
     */
    public static String toJsonForObject(final Object liteJobConfig) {
        return GsonFactory.getGson().toJson(liteJobConfig);
    }

    /**
     * 将JSON字符串转换为作业配置.
     *
     * @param liteJobConfigJson 作业配置JSON字符串
     * @return 作业配置对象
     */
    public static JobConfiguration fromJson(final String liteJobConfigJson) {
        return GsonFactory.getGson().fromJson(liteJobConfigJson, JobConfiguration.class);
    }

    /**
     * Lite作业配置的Json转换适配器.
     *
     * @author zhangliang
     */
    static final class LiteJobConfigurationGsonTypeAdapter extends
            AbstractJobConfigurationGsonTypeAdapter<JobConfiguration> {

        @Override
        protected void addToCustomizedValueMap(final String jsonName, final JsonReader in, final Map<String, Object>
                customizedValueMap) throws IOException {
            String jobParameter = "";
            boolean failover = false;
            boolean misfire = failover;
            String description = "";
            JobProperties jobProperties = new JobProperties();

            switch (jsonName) {
                case "monitorExecution":
                    customizedValueMap.put(jsonName, in.nextBoolean());
                    break;
                case "maxTimeDiffSeconds":
                    customizedValueMap.put(jsonName, in.nextInt());
                    break;
                case "monitorPort":
                    customizedValueMap.put(jsonName, in.nextInt());
                    break;
                case "jobShardingStrategyClass":
                    customizedValueMap.put(jsonName, in.nextString());
                    break;
                case "reconcileIntervalMinutes":
                    customizedValueMap.put(jsonName, in.nextInt());
                    break;
                case "disabled":
                    customizedValueMap.put(jsonName, in.nextBoolean());
                    break;
                case "overwrite":
                    customizedValueMap.put(jsonName, in.nextBoolean());
                    break;
                case "jobParameter":
                    jobParameter = in.nextString();
                    customizedValueMap.put(jsonName, jobParameter);
                    break;
                case "failover":
                    failover = in.nextBoolean();
                    customizedValueMap.put(jsonName, failover);
                    break;
                case "misfire":
                    misfire = in.nextBoolean();
                    customizedValueMap.put(jsonName, misfire);
                    break;
                case "description":
                    description = in.nextString();
                    customizedValueMap.put(jsonName, description);
                    break;
                case "jobProperties":
                    jobProperties = getJobProperties(in);
                    customizedValueMap.put(jsonName, jobProperties);
                    break;
                default:
                    in.skipValue();
                    break;
            }
        }

        private JobProperties getJobProperties(final JsonReader in) throws IOException {
            JobProperties result = new JobProperties();
            in.beginObject();
            while (in.hasNext()) {
                switch (in.nextName()) {
                    case "job_exception_handler":
                        result.put(JobProperties.JobPropertiesEnum.JOB_EXCEPTION_HANDLER.getKey(), in.nextString());
                        break;
                    case "executor_service_handler":
                        result.put(JobProperties.JobPropertiesEnum.EXECUTOR_SERVICE_HANDLER.getKey(), in.nextString());
                        break;
                    default:
                        break;
                }
            }
            in.endObject();
            return result;
        }

        @Override
        protected JobConfiguration getJobConfiguration(final JobDefinition jobDefinition, final Map<String, Object>
                customizedValueMap) {
            JobConfiguration.Builder builder = JobConfiguration.newBuilder(jobDefinition);
            if (customizedValueMap.containsKey("monitorExecution")) {
                builder.monitorExecution((boolean) customizedValueMap.get("monitorExecution"));
            }
            if (customizedValueMap.containsKey("maxTimeDiffSeconds")) {
                builder.maxTimeDiffSeconds((int) customizedValueMap.get("maxTimeDiffSeconds"));
            }
            if (customizedValueMap.containsKey("monitorPort")) {
                builder.monitorPort((int) customizedValueMap.get("monitorPort"));
            }
            if (customizedValueMap.containsKey("jobShardingStrategyClass")) {
                builder.jobShardingStrategyClass((String) customizedValueMap.get("jobShardingStrategyClass"));
            }
            if (customizedValueMap.containsKey("reconcileIntervalMinutes")) {
                builder.reconcileIntervalMinutes((int) customizedValueMap.get("reconcileIntervalMinutes"));
            }
            if (customizedValueMap.containsKey("disabled")) {
                builder.disabled((boolean) customizedValueMap.get("disabled"));
            }
            if (customizedValueMap.containsKey("overwrite")) {
                builder.overwrite((boolean) customizedValueMap.get("overwrite"));
            }

            if (customizedValueMap.containsKey("jobParameter")) {
                builder.jobParameter((String) customizedValueMap.get("jobParameter"));
            }

            if (customizedValueMap.containsKey("failover")) {
                builder.failover((boolean) customizedValueMap.get("failover"));
            }

            if (customizedValueMap.containsKey("misfire")) {
                builder.misfire((boolean) customizedValueMap.get("misfire"));
            }

            if (customizedValueMap.containsKey("description")) {
                builder.description((String) customizedValueMap.get("description"));
            }

            if (customizedValueMap.containsKey("jobProperties")) {
                JobProperties jobProperties = (JobProperties) customizedValueMap.get("jobProperties");
                builder.jobProperties(JobProperties.JobPropertiesEnum.JOB_EXCEPTION_HANDLER.getKey(), jobProperties.get
                        (JobProperties.JobPropertiesEnum.JOB_EXCEPTION_HANDLER))
                        .jobProperties(JobProperties
                                .JobPropertiesEnum.EXECUTOR_SERVICE_HANDLER.getKey(), jobProperties.get
                                (JobProperties.JobPropertiesEnum.EXECUTOR_SERVICE_HANDLER));
            }
            return builder.build();
        }

        @Override
        protected void writeCustomized(final JsonWriter out, final JobConfiguration value) throws IOException {
            out.name("monitorExecution").value(value.isMonitorExecution());
            out.name("maxTimeDiffSeconds").value(value.getMaxTimeDiffSeconds());
            out.name("monitorPort").value(value.getMonitorPort());
            out.name("jobShardingStrategyClass").value(value.getJobShardingStrategyClass());
            out.name("reconcileIntervalMinutes").value(value.getReconcileIntervalMinutes());
            out.name("disabled").value(value.isDisabled());
            out.name("overwrite").value(value.isOverwrite());


            out.name("jobParameter").value(value.getJobParameter());
            out.name("failover").value(value.isFailover());
            out.name("misfire").value(value.isMisfire());
            out.name("description").value(value.getDescription());
            out.name("jobProperties").jsonValue(value.getJobProperties().json());
        }
    }
}
