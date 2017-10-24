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
import com.jeeframework.util.string.apache.StringUtils;
import com.jeeframework.util.validate.Validate;

import java.util.ArrayList;
import java.util.List;

/**
 * ip工具类
 *
 * @author lance
 * @version 1.0 2017-07-31 12:04
 */
public class IPUtils {

    private final static String outNetIPv4;
    private final static String localIPv4;
    private final static String macAddress;

    static {
        localIPv4 = IPUtil.getLocalIpV4();
        outNetIPv4 = IPUtil.getOutNetIPV4();
        String localMac = IPUtil.getMACByIp(localIPv4);
        String outMac = IPUtil.getMACByIp(outNetIPv4);
        macAddress = !Validate.isEmpty(localMac) ? localMac : outMac;
    }

    public static String getUniqueServerId() {
        String serverId = null;
        try {
            serverId = System.getProperty("serverId");
        } catch (Exception e) {
        }
        if (!Validate.isEmpty(serverId)) {
            return serverId.trim();
        }

        List<String> uniqueIdList = new ArrayList<>();
        if (!Validate.isEmpty(outNetIPv4)) {
            uniqueIdList.add(outNetIPv4);
        }
        if (!Validate.isEmpty(localIPv4)) {
            uniqueIdList.add(localIPv4);
        }
        if (!Validate.isEmpty(macAddress)) {
            uniqueIdList.add(macAddress);
        }
        return StringUtils.join(uniqueIdList, "_");
    }
}
