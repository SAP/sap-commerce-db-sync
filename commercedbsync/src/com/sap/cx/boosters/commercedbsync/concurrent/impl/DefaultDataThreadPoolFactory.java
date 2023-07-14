/*
 *  Copyright: 2023 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.concurrent.impl;

import com.sap.cx.boosters.commercedbsync.context.CopyContext;
import com.sap.cx.boosters.commercedbsync.DataThreadPoolConfig;
import com.sap.cx.boosters.commercedbsync.concurrent.DataThreadPoolFactory;
import com.sap.cx.boosters.commercedbsync.concurrent.DataThreadPoolMonitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.task.TaskDecorator;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

public class DefaultDataThreadPoolFactory implements DataThreadPoolFactory {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultDataThreadPoolFactory.class);
    private static final int MAX_CAPACITY = 2147483647;

    private final DataThreadPoolMonitor monitor;

    private final TaskDecorator taskDecorator;
    private final String threadNamePrefix;
    private final int corePoolSize;
    private final int maxPoolSize;
    private final int keepAliveSeconds;
    private final boolean allowCoreThreadTimeOut;
    private final boolean waitForTasksToCompleteOnShutdown;
    private int queueCapacity = MAX_CAPACITY;

    public DefaultDataThreadPoolFactory(final TaskDecorator taskDecorator, final String threadNamePrefix,
            final int maxPoolSize, final int keepAliveSeconds, final boolean allowCoreThreadTimeOut,
            final boolean waitForTasksToCompleteOnShutdown, final boolean queueable) {
        this.monitor = new DefaultDataThreadPoolMonitor();
        this.taskDecorator = taskDecorator;
        this.threadNamePrefix = threadNamePrefix;
        this.maxPoolSize = maxPoolSize;
        this.keepAliveSeconds = keepAliveSeconds;
        this.allowCoreThreadTimeOut = allowCoreThreadTimeOut;
        this.waitForTasksToCompleteOnShutdown = waitForTasksToCompleteOnShutdown;
        this.queueCapacity = getQueueCapacity(queueable);
        this.corePoolSize = maxPoolSize;
    }

    @Override
    public ThreadPoolTaskExecutor create(final CopyContext context, final DataThreadPoolConfig config) {
        final ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setTaskDecorator(taskDecorator);
        executor.setThreadNamePrefix(threadNamePrefix);

        executor.setCorePoolSize(defaultIfNull(config.getPoolSize(), corePoolSize));
        executor.setMaxPoolSize(defaultIfNull(config.getPoolSize(), corePoolSize));
        executor.setQueueCapacity(queueCapacity);
        executor.setKeepAliveSeconds(keepAliveSeconds);
        executor.setAllowCoreThreadTimeOut(allowCoreThreadTimeOut);
        executor.setWaitForTasksToCompleteOnShutdown(waitForTasksToCompleteOnShutdown);
        if (waitForTasksToCompleteOnShutdown) {
            executor.setAwaitTerminationSeconds(Integer.MAX_VALUE);
        }
        executor.initialize();
        monitor.subscribe(executor);
        LOG.debug("Creating executor with parameters: Pool Size:{}", corePoolSize);
        return executor;
    }

    @Override
    public void destroy(ThreadPoolTaskExecutor executor) {
        executor.shutdown();
        monitor.unsubscribe(executor);
    }

    @Override
    public DataThreadPoolMonitor getMonitor() {
        return monitor;
    }

    private <T> T defaultIfNull(T value, T def) {
        if (value == null) {
            return def;
        } else {
            return value;
        }
    }

    private int getQueueCapacity(boolean queueable) {
        return queueable ? this.queueCapacity : 0;
    }
}
