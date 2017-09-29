package com.jeeframework.jeetask.util.net;

import com.jeeframework.util.validate.Validate;
import org.junit.Assert;
import org.junit.Test;

/**
 * 描述
 *
 * @author lance
 * @version 1.0 2017-09-13 19:12
 */
public class IPUtilsTest {
    @Test
    public void getUniqueServerId() throws Exception {
        Assert.assertTrue(Validate.isEmpty(IPUtils.getUniqueServerId()));
    }

}