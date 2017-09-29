/*
 * @project: jeetask 
 * @package: com.jeeframework.jeetask.startup.server
 * @title:   JeeTaskServerChild.java 
 *
 * Copyright (c) 2017 jeeframework Limited, Inc.
 * All rights reserved.
 */
package com.jeeframework.jeetask.startup.server;

import com.jeeframework.jeetask.startup.JeeTaskServer;

import javax.sql.DataSource;
import java.io.IOException;

/**
 * 描述
 *
 * @author lance
 * @version 1.0 2017-09-29 17:47
 */
public class JeeTaskServerChild extends JeeTaskServer {
    public JeeTaskServerChild(DataSource taskDataSource) throws IOException {
        super(taskDataSource);
    }

    public JeeTaskServerChild() {
    }

    protected int getMaxAllowedWorkerCount() {
        return 2;
    }


}
