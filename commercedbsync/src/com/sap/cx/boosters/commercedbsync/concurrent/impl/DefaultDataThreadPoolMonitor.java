/*
 *  Copyright: 2023 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.concurrent.impl;

import com.sap.cx.boosters.commercedbsync.concurrent.DataThreadPoolMonitor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 *
 */
public class DefaultDataThreadPoolMonitor implements DataThreadPoolMonitor {

    private final List<ThreadPoolTaskExecutor> executors;

    public DefaultDataThreadPoolMonitor() {
        this.executors = Collections.synchronizedList(new ArrayList<>());
    }

    @Override
    public void subscribe(ThreadPoolTaskExecutor executor) {
        executors.add(executor);
    }

    @Override
    public void unsubscribe(ThreadPoolTaskExecutor executor) {
        executors.remove(executor);
    }

    @Override
    public int getActiveCount() {
        return executors.stream().mapToInt(ThreadPoolTaskExecutor::getActiveCount).sum();
    }

    @Override
    public int getMaxPoolSize() {
        return executors.stream().mapToInt(ThreadPoolTaskExecutor::getMaxPoolSize).sum();
    }

}
