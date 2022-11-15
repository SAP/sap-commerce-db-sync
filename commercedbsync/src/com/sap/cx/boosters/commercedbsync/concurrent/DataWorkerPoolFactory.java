/*
 *  Copyright: 2022 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.concurrent;

import com.sap.cx.boosters.commercedbsync.context.CopyContext;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

public interface DataWorkerPoolFactory {
    ThreadPoolTaskExecutor create(CopyContext context);
}
