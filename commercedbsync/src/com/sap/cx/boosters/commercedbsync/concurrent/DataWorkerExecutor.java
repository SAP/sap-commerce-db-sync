/*
 *  Copyright: 2022 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.concurrent;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public interface DataWorkerExecutor<T> {
    Future<T> safelyExecute(Callable<T> callable) throws InterruptedException;

    void waitAndRethrowUncaughtExceptions() throws ExecutionException, InterruptedException;
}
