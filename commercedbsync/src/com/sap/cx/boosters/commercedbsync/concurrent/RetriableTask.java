/*
 *  Copyright: 2022 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.concurrent;

import com.sap.cx.boosters.commercedbsync.context.CopyContext;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;

public abstract class RetriableTask implements Callable<Boolean> {

    private static final Logger LOG = LoggerFactory.getLogger(RetriableTask.class);

    private CopyContext context;
    private String table;
    private int retryCount = 0;

    public RetriableTask(CopyContext context, String table) {
        this.context = context;
        this.table = table;
    }

    @Override
    public Boolean call() {
        try {
            return internalRun();
        } catch (PipeAbortedException e) {
            throw new RuntimeException("Ignore retries", e);
        } catch (Exception e) {
            if (retryCount < context.getMigrationContext().getMaxWorkerRetryAttempts()) {
                LOG.warn("Retrying failed task {} for table {}. Retry count: {}. Cause: {}", getClass().getName(), table, retryCount, e);
                e.printStackTrace();
                retryCount++;
                return call();
            } else {
                handleFailure(e);
                return Boolean.FALSE;
            }
        }
    }

    protected void handleFailure(Exception e) {
        throw new RuntimeException(ExceptionUtils.getRootCauseMessage(e), e);
    }

    protected abstract Boolean internalRun() throws Exception;

}
