package com.jeeframework.jeetask.startup;

import com.jeeframework.jeetask.startup.task.TestTask;
import org.junit.Test;

/**
 * 描述
 *
 * @author lance
 * @version 1.0 2017-08-17 18:50
 */
public class JeeTaskClientTest {
    @Test
    public void stopTask() throws Exception {
        JeeTaskClient jeeTaskClient = new JeeTaskClient(null);
        jeeTaskClient.start();

        jeeTaskClient.stopTask(111L);
    }

    @Test
    public void submitTask() throws Exception {

        TestTask task = new TestTask();

        String taskName = task.getClass().getName();

        task.setName("taskName8");
        task.setParam("param8");
        task.setJobClass("com.jeeframework.jeetask.startup.job.JobImpl");

        JeeTaskClient jeeTaskClient = new JeeTaskClient(null);
        jeeTaskClient.start();

        jeeTaskClient.submitTask(task);
        System.out.println(task.getId());
    }

}