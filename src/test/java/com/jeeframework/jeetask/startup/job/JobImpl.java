/*
 * @project: jeetask 
 * @package: com.jeeframework.jeetask.startup.job
 * @title:   JobImpl.java 
 *
 * Copyright (c) 2017 jeeframework Limited, Inc.
 * All rights reserved.
 */
package com.jeeframework.jeetask.startup.job;

import com.jeeframework.jeetask.task.Job;

/**
 * 作业实现类
 *
 * @author lance
 * @version 1.0 2017-08-25  14:57
 */
public class JobImpl implements Job {
    @Override
    public void doJob() {
        int i = 0;
        while (true) {
            System.out.println("dojob  =   " + i);

            i = i + 1;
            if (i > 5) {
                break;
            }

            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        }

    }
}
