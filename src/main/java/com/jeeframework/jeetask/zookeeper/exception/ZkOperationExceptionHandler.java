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

package com.jeeframework.jeetask.zookeeper.exception;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.ConnectionLossException;

/**
 * 注册中心异常处理类.
 *
 * @author zhangliang
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class ZkOperationExceptionHandler {

    /**
     * 处理异常.
     * <p>
     * <p>处理掉中断和连接失效异常并继续抛注册中心.</p>
     *
     * @param cause 待处理异常.
     */
    public static void handleExceptionNoOrExistNode(final Exception cause) {
        if (isIgnoredExceptionNoOrExistNode(cause) || null != cause.getCause() && isIgnoredExceptionNoOrExistNode(cause.getCause())) {
            log.debug("Elastic job: ignored exception for: {}", cause.getMessage());
        } else if (cause instanceof InterruptedException) {
            Thread.currentThread().interrupt();
        } else {
            throw new ZkOperationException(cause);
        }
    }

    private static boolean isIgnoredExceptionNoOrExistNode(final Throwable cause) {
        return null != cause && (cause instanceof ConnectionLossException || cause instanceof KeeperException.NoNodeException ||
                cause instanceof KeeperException.NodeExistsException);
    }

    public static void handleException(final Exception cause) {
        if (isIgnoredException(cause) || null != cause.getCause() && isIgnoredException(cause.getCause())) {
            log.debug("Elastic job: ignored exception for: {}", cause.getMessage());
        } else if (cause instanceof InterruptedException) {
            Thread.currentThread().interrupt();
        } else {
            throw new ZkOperationException(cause);
        }
    }

    private static boolean isIgnoredException(final Throwable cause) {
//        return null != cause && (cause instanceof ConnectionLossException || cause instanceof NoNodeException ||
// cause instanceof NodeExistsException);
        return null != cause && (cause instanceof ConnectionLossException);
    }
}
