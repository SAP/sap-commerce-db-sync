/*
 *  Copyright: 2023 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.concurrent.impl.task;

public abstract class DataReaderTask extends RetriableTask {

    private final PipeTaskContext pipeTaskContext;

    protected DataReaderTask(PipeTaskContext pipeTaskContext) {
        super(pipeTaskContext.getContext(), pipeTaskContext.getTable());
        this.pipeTaskContext = pipeTaskContext;
    }

    public PipeTaskContext getPipeTaskContext() {
        return pipeTaskContext;
    }
}
