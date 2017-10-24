/*
 * @project: jeetask 
 * @package: com.jeeframework.jeetask.event.rdb.impl
 * @title:   JobEventMysqlStorage.java 
 *
 * Copyright (c) 2017 jeeframework Limited, Inc.
 * All rights reserved.
 */
package com.jeeframework.jeetask.event.rdb.impl;

import com.jeeframework.jeetask.event.exception.JobEventException;
import com.jeeframework.jeetask.event.rdb.JobEventStorage;
import com.jeeframework.jeetask.event.type.JobExecutionEvent;
import com.jeeframework.util.format.DateFormat;
import lombok.extern.slf4j.Slf4j;

import javax.sql.DataSource;
import java.sql.*;
import java.text.ParseException;

/**
 * Mysql h2 通用作业调度处理类
 *
 * @author lance
 * @version 1.0 2017-08-25 17:19
 */
@Slf4j
public class JobEventCommonStorageProcessor extends JobEventStorage {
    private final static String TASK_TABLE_NAME = "t_task";

    public JobEventCommonStorageProcessor(DataSource dataSource) {
        super(dataSource);
    }

    @Override
    protected String getTaskTableName() {
        return TASK_TABLE_NAME;
    }

    protected void createTaskTable(final Connection conn) throws SQLException {
        String dbSchema = "CREATE TABLE `" + tableName + "` ("
                + "`id` bigint(20) NOT NULL AUTO_INCREMENT, "
                + "`job_name` VARCHAR(100) NOT NULL, "
                + "`state` VARCHAR(20) NOT NULL, "
                + "`progress` INT NOT NULL, "
                + "`ip` VARCHAR(20) NOT NULL, "
                + "`param` VARCHAR(500) NOT NULL, "
                + "`message` VARCHAR(500) NOT NULL, "
                + "`create_time` TIMESTAMP NULL, "
                + "`start_time` TIMESTAMP NULL, "
                + "`complete_time` TIMESTAMP NULL, "
                + "`failure_cause` VARCHAR(4000) NULL, "
                + "PRIMARY KEY (`id`));";
        try (PreparedStatement preparedStatement = conn.prepareStatement(dbSchema)) {
            preparedStatement.execute();
        }
    }

    protected void createTaskIdAndStateIndex(final Connection conn, final String tableName, final String indexName)
            throws SQLException {
        String sql = "CREATE INDEX " + indexName + " ON " + tableName + " (`id`, `state`);";
        try (PreparedStatement preparedStatement = conn.prepareStatement(sql)) {
            preparedStatement.execute();
        }
    }


    protected boolean insertJobExecutionEvent(final JobExecutionEvent jobExecutionEvent) {
        //    id	bigint	20	0	0	-1	0	0	0		0					-1	0
//    failure_cause	varchar	4000	0	-1	0	0	0	0		0	失败堆栈	utf8	utf8_general_ci		0	0

        boolean result = false;
        String sql = "INSERT INTO `" + tableName + "` (`job_name`, `state`, `progress`, `ip`, " +
                "`param`, `message`, `create_time`, `start_time`, `complete_time`) "
                + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);";
        try (
                Connection conn = dataSource.getConnection();
                PreparedStatement preparedStatement = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)) {
            preparedStatement.setString(1, jobExecutionEvent.getTaskName());
            preparedStatement.setString(2, jobExecutionEvent.getState().toString());
            preparedStatement.setInt(3, 0);
            preparedStatement.setString(4, jobExecutionEvent.getIp());
            preparedStatement.setString(5, jobExecutionEvent.getParam());
            preparedStatement.setString(6, "");
            preparedStatement.setTimestamp(7, new Timestamp(jobExecutionEvent.getCreateTime().getTime()));
            try {
                preparedStatement.setTimestamp(8, new Timestamp(DateFormat.parseDate("1971-01-01 00:00:00", DateFormat
                        .DT_YYYY_MM_DD_HHMMSS).getTime()));
            } catch (ParseException e) {
            }
            try {
                preparedStatement.setTimestamp(9, new Timestamp(DateFormat.parseDate("1971-01-01 00:00:00", DateFormat
                        .DT_YYYY_MM_DD_HHMMSS).getTime()));
            } catch (ParseException e) {
            }
            preparedStatement.execute();
            ResultSet rs = preparedStatement.getGeneratedKeys();

            Object retId = null;
            if (rs.next())
                retId = rs.getObject(1);
            else
                throw new SQLException("insert or generate keys failed..");

            jobExecutionEvent.setTaskId(Long.valueOf(retId + ""));
            result = true;
        } catch (final SQLException ex) {
            if (isDuplicateRecord(ex)) {
                log.error(ex.getMessage());
            } else {
                throw new JobEventException(ex);
            }
        }
        return result;
    }

    protected boolean updateJobExecutionEventWhenStaging(final JobExecutionEvent jobExecutionEvent) {
        boolean result = false;
        String sql = "UPDATE `" + tableName + "` SET `state` = ?   WHERE id = ?";
        try (
                Connection conn = dataSource.getConnection();
                PreparedStatement preparedStatement = conn.prepareStatement(sql)) {
            preparedStatement.setString(1, jobExecutionEvent.getState().toString());
            preparedStatement.setLong(2, jobExecutionEvent.getTaskId());
            preparedStatement.executeUpdate();
            result = true;
        } catch (final SQLException ex) {
            throw new JobEventException(ex);
        }
        return result;
    }

    protected boolean updateJobExecutionEventWhenStart(final JobExecutionEvent jobExecutionEvent) {
        boolean result = false;
        String sql = "UPDATE `" + tableName + "` SET `state` = ?,`start_time` = ?   WHERE id = ?";
        try (
                Connection conn = dataSource.getConnection();
                PreparedStatement preparedStatement = conn.prepareStatement(sql)) {
            preparedStatement.setString(1, jobExecutionEvent.getState().toString());
            preparedStatement.setTimestamp(2, new Timestamp(jobExecutionEvent.getStartTime().getTime()));
            preparedStatement.setLong(3, jobExecutionEvent.getTaskId());
            preparedStatement.executeUpdate();
            result = true;
        } catch (final SQLException ex) {
            throw new JobEventException(ex);
        }
        return result;
    }

    protected boolean updateJobExecutionEventWhenFinished(final JobExecutionEvent jobExecutionEvent) {
        boolean result = false;
        String sql = "UPDATE `" + tableName + "` SET `state` = ?,`complete_time` = ?   WHERE id = ?";
        try (
                Connection conn = dataSource.getConnection();
                PreparedStatement preparedStatement = conn.prepareStatement(sql)) {
            preparedStatement.setString(1, jobExecutionEvent.getState().toString());
            preparedStatement.setTimestamp(2, new Timestamp(jobExecutionEvent.getCompleteTime().getTime()));
            preparedStatement.setLong(3, jobExecutionEvent.getTaskId());
            preparedStatement.executeUpdate();
            result = true;
        } catch (final SQLException ex) {
            throw new JobEventException(ex);
        }
        return result;
    }

    protected boolean updateJobExecutionEventFailure(final JobExecutionEvent jobExecutionEvent) {
        boolean result = false;
        String sql = "UPDATE `" + tableName + "` SET `state` = ?, `complete_time` = ?, " +
                "`failure_cause` = ? WHERE id = ?";
        try (
                Connection conn = dataSource.getConnection();
                PreparedStatement preparedStatement = conn.prepareStatement(sql)) {
            preparedStatement.setString(1, jobExecutionEvent.getState().toString());
            preparedStatement.setTimestamp(2, new Timestamp(jobExecutionEvent.getCompleteTime().getTime()));
            preparedStatement.setString(3, truncateString(jobExecutionEvent.getFailureCause()));
            preparedStatement.setLong(4, jobExecutionEvent.getTaskId());
            preparedStatement.executeUpdate();
            result = true;
        } catch (final SQLException ex) {
            throw new JobEventException(ex);
        }
        return result;
    }

    @Override
    protected boolean updateJobExecutionEventStopped(JobExecutionEvent jobExecutionEvent) {
        boolean result = false;
        String sql = "UPDATE `" + tableName + "` SET `state` = ?,`complete_time` = ?   WHERE id = ?";
        try (
                Connection conn = dataSource.getConnection();
                PreparedStatement preparedStatement = conn.prepareStatement(sql)) {
            preparedStatement.setString(1, jobExecutionEvent.getState().toString());
            preparedStatement.setTimestamp(2, new Timestamp(jobExecutionEvent.getCompleteTime().getTime()));
            preparedStatement.setLong(3, jobExecutionEvent.getTaskId());
            preparedStatement.executeUpdate();
            result = true;
        } catch (final SQLException ex) {
            throw new JobEventException(ex);
        }
        return result;
    }

    @Override
    protected boolean updateJobExecutionEventRecovery(JobExecutionEvent jobExecutionEvent) {
        boolean result = false;
        String sql = "UPDATE `" + tableName + "` SET `state` = ?   WHERE id = ?";
        try (
                Connection conn = dataSource.getConnection();
                PreparedStatement preparedStatement = conn.prepareStatement(sql)) {
            preparedStatement.setString(1, jobExecutionEvent.getState().toString());
            preparedStatement.setLong(2, jobExecutionEvent.getTaskId());
            preparedStatement.executeUpdate();
            result = true;
        } catch (final SQLException ex) {
            throw new JobEventException(ex);
        }
        return result;
    }
}
