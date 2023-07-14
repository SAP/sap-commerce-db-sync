/*
 *  Copyright: 2023 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.concurrent;

import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

public interface DataThreadPoolMonitor {
    void subscribe(ThreadPoolTaskExecutor executor);

    void unsubscribe(ThreadPoolTaskExecutor executor);

    int getActiveCount();

    int getMaxPoolSize();
}
