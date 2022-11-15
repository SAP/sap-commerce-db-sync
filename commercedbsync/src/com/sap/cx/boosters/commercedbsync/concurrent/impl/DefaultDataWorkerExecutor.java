/*
 *  Copyright: 2022 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.concurrent.impl;

import com.sap.cx.boosters.commercedbsync.concurrent.DataWorkerExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.core.task.TaskRejectedException;
import org.springframework.util.backoff.BackOffExecution;
import org.springframework.util.backoff.ExponentialBackOff;

import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class DefaultDataWorkerExecutor<T> implements DataWorkerExecutor<T> {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultDataWorkerExecutor.class);

    private AsyncTaskExecutor executor;
    private Queue<Future<T>> futures = new ArrayDeque<>();


    public DefaultDataWorkerExecutor(AsyncTaskExecutor executor) {
        this.executor = executor;
    }

    @Override
    public Future<T> safelyExecute(Callable<T> callable) throws InterruptedException {
        Future<T> future = internalSafelyExecute(callable, 0);
        futures.add(future);
        return future;
    }

    private Future<T> internalSafelyExecute(Callable<T> callable, int rejections) throws InterruptedException {
        try {
            return executor.submit(callable);
        } catch (TaskRejectedException e) {
            BackOffExecution backOff = new ExponentialBackOff().start();
            long waitInterval = backOff.nextBackOff();
            for (int i = 0; i < rejections; i++) {
                waitInterval = backOff.nextBackOff();
            }
            LOG.trace("worker rejected. Retrying in {}ms...", waitInterval);
            Thread.sleep(waitInterval);
            return internalSafelyExecute(callable, rejections + 1);
        }
    }

    @Override
    public void waitAndRethrowUncaughtExceptions() throws ExecutionException, InterruptedException {
        Future<T> future;
        while ((future = futures.poll()) != null) {
            future.get();
        }
    }
}
