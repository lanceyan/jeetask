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

package com.jeeframework.jeetask.event.rdb;

import com.google.common.base.Strings;
import com.jeeframework.jeetask.event.type.JobExecutionEvent;
import com.jeeframework.jeetask.event.type.JobStatusTraceEvent;
import lombok.extern.slf4j.Slf4j;

import javax.sql.DataSource;
import java.sql.*;


/**
 * 运行痕迹事件数据库存储.
 *
 * @author caohao
 */
@Slf4j
public abstract class JobEventStorage {

    //    private static final String tableName = "t_task";
//
//    private static final String TABLE_JOB_STATUS_TRACE_LOG = "t_task_status_trace_log";

    public final static String DB_TYPE_H2 = "h2";
    public final static String DB_TYPE_MYSQL = "mysql";
    //
    protected static final String TASK_ID_INDEX = "idx_task_id_state";

    protected final DataSource dataSource;

    private DatabaseType databaseType;

    protected String tableName;

    protected abstract String getTaskTableName();

    public JobEventStorage(final DataSource dataSource) {
        this.dataSource = dataSource;
        this.tableName = getTaskTableName();
        try {
            initTablesAndIndexes();
        } catch (SQLException e) {
            log.error("初始化任务表结构失败，错误信息= " + e, e);
        }
    }

    private void initTablesAndIndexes() throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            databaseType = DatabaseType.valueFrom(conn.getMetaData().getDatabaseProductName());
            createTaskTableAndIndexIfNeeded(conn);
//            createJobStatusTraceTableAndIndexIfNeeded(conn);

        }
    }

    private void createTaskTableAndIndexIfNeeded(final Connection conn) throws SQLException {
//        DatabaseMetaData dbMetaData = conn.getMetaData();
//        try (ResultSet resultSet = dbMetaData.getTables(null, null, tableName, new String[]{"TABLE"})) {
//            if (!resultSet.next()) {
        try {
            createTaskTable(conn);
        } catch (SQLException e) {
            if (isExistsTable(e)) {
                log.error("表" + tableName + "已经存在，不重复创建");
            } else {
                throw e;
            }
        }
//    }

        //        }


        try {
            createTaskIdIndexIfNeeded(conn, tableName, TASK_ID_INDEX);
        } catch (SQLException e) {
            if (isExistsIndex(e)) {
                log.error("表索引" + TASK_ID_INDEX + "已经存在，不重复创建");
            } else {
                throw e;
            }
        }


    }

//    private void createJobStatusTraceTableAndIndexIfNeeded(final Connection conn) throws SQLException {
//        DatabaseMetaData dbMetaData = conn.getMetaData();
//        try (ResultSet resultSet = dbMetaData.getTables(null, null, TABLE_JOB_STATUS_TRACE_LOG, new
//                String[]{"TABLE"})) {
//            if (!resultSet.next()) {
//                createJobStatusTraceTable(conn);
//            }
//        }
//        createTaskIdIndexIfNeeded(conn, TABLE_JOB_STATUS_TRACE_LOG, TASK_ID_STATE_INDEX);
//    }

    private void createTaskIdIndexIfNeeded(final Connection conn, final String tableName, final String indexName)
            throws SQLException {
//        DatabaseMetaData dbMetaData = conn.getMetaData();
//        try (ResultSet resultSet = dbMetaData.getIndexInfo(null, null, tableName, false, false)) {
//            boolean hasTaskIdIndex = false;
//            while (resultSet.next()) {
//                if (indexName.equals(resultSet.getString("INDEX_NAME"))) {
//                    hasTaskIdIndex = true;
//                }
//            }
//            if (!hasTaskIdIndex) {
        createTaskIdAndStateIndex(conn, tableName, indexName);
//            }
//        }
    }

    protected abstract void createTaskTable(final Connection conn) throws SQLException;

//    private void createJobStatusTraceTable(final Connection conn) throws SQLException {
//        String dbSchema = "CREATE TABLE `" + TABLE_JOB_STATUS_TRACE_LOG + "` ("
//                + "`id` bigint(20) NOT NULL, "
//                + "`job_name` VARCHAR(100) NOT NULL, "
//                + "`task_id` bigint(20) NOT NULL, "
//                + "`state` INT NOT NULL, "
//                + "`createtime` TIMESTAMP NULL, "
//                + "PRIMARY KEY (`id`));";
//        try (PreparedStatement preparedStatement = conn.prepareStatement(dbSchema)) {
//            preparedStatement.execute();
//        }
//    }

    protected abstract void createTaskIdAndStateIndex(final Connection conn, final String tableName, final String
            indexName) throws SQLException;

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
        }

        return true;

//        if (null == jobExecutionEvent.getEndTime()) {
//            return updateJobExecutionEvent(jobExecutionEvent);
//        } else {
//            if (jobExecutionEvent.isSuccess()) {
//                return updateJobExecutionEventWhenSuccess(jobExecutionEvent);
//            } else {
//                return updateJobExecutionEventFailure(jobExecutionEvent);
//            }
//        }
    }

    protected abstract boolean insertJobExecutionEvent(final JobExecutionEvent jobExecutionEvent);

    protected abstract boolean updateJobExecutionEventWhenStaging(final JobExecutionEvent jobExecutionEvent);

    protected abstract boolean updateJobExecutionEventWhenStart(final JobExecutionEvent jobExecutionEvent);

    protected abstract boolean updateJobExecutionEventWhenFinished(final JobExecutionEvent jobExecutionEvent);

    protected abstract boolean updateJobExecutionEventFailure(final JobExecutionEvent jobExecutionEvent);

    protected String truncateString(final String str) {
        return !Strings.isNullOrEmpty(str) && str.length() > 4000 ? str.substring(0, 4000) : str;
    }

    protected boolean isDuplicateRecord(final SQLException ex) {
        return DatabaseType.MySQL.equals(databaseType) && 1062 == ex.getErrorCode() || DatabaseType.H2.equals
                (databaseType) && 23505 == ex.getErrorCode()
                || DatabaseType.SQLServer.equals(databaseType) && 1 == ex.getErrorCode() || DatabaseType.DB2.equals
                (databaseType) && -803 == ex.getErrorCode()
                || DatabaseType.PostgreSQL.equals(databaseType) && 0 == ex.getErrorCode() || DatabaseType.Oracle
                .equals(databaseType) && 1 == ex.getErrorCode();
    }

    protected boolean isExistsTable(final SQLException ex) {
//        vendorCode = 1050
//        next = null
//        detailMessage = "Table 't_task' already exists"
        return DatabaseType.MySQL.equals(databaseType) && 1050 == ex.getErrorCode();
    }

    protected boolean isExistsIndex(final SQLException ex) {
//        vendorCode = 1061
//        next = null
//        detailMessage = "Duplicate key name 'idx_task_id_state'"
        return DatabaseType.MySQL.equals(databaseType) && 1061 == ex.getErrorCode();
    }

}
