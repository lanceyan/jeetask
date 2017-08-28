/*
 * @project: jeetask 
 * @package: com.jeeframework.jeetask.util.json
 * @title:   IPUtils.java 
 *
 * Copyright (c) 2017 jeeframework Limited, Inc.
 * All rights reserved.
 */
package com.jeeframework.jeetask.util.net;

import com.jeeframework.util.net.IPUtil;

/**
 * ip工具类
 *
 * @author lance
 * @version 1.0 2017-07-31 12:04
 */
public class IPUtils {

    private final static String outNetIPv4;
    private final static String localIPv4;

    static {
        localIPv4 = IPUtil.getLocalIpV4();
        outNetIPv4 = IPUtil.getOutNetIPV4();
    }

    public static String getOutAndLocalIPV4() {
        return outNetIPv4 + "_" + localIPv4;
    }
}
