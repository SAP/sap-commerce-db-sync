/*
 *  Copyright: 2023 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.concurrent;

import com.sap.cx.boosters.commercedbsync.context.CopyContext;
import com.sap.cx.boosters.commercedbsync.DataThreadPoolConfig;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

public interface DataThreadPoolFactory {
    ThreadPoolTaskExecutor create(CopyContext context, DataThreadPoolConfig config);

    void destroy(ThreadPoolTaskExecutor executor);

    DataThreadPoolMonitor getMonitor();
}
