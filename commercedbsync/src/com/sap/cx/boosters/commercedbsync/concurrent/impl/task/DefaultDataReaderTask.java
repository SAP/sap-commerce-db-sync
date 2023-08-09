/*
 *  Copyright: 2023 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.concurrent.impl.task;

import com.sap.cx.boosters.commercedbsync.concurrent.MaybeFinished;
import com.sap.cx.boosters.commercedbsync.context.MigrationContext;
import com.sap.cx.boosters.commercedbsync.dataset.DataSet;
import com.sap.cx.boosters.commercedbsync.performance.PerformanceUnit;

public class DefaultDataReaderTask extends DataReaderTask {

    public DefaultDataReaderTask(PipeTaskContext pipeTaskContext) {
        super(pipeTaskContext);
    }

    @Override
    protected Boolean internalRun() throws Exception {
        process();
        return Boolean.TRUE;
    }

    private void process() throws Exception {
        MigrationContext migrationContext = getPipeTaskContext().getContext().getMigrationContext();
        DataSet all = getPipeTaskContext().getDataRepositoryAdapter().getAll(migrationContext,
                getPipeTaskContext().getTable());
        getPipeTaskContext().getRecorder().record(PerformanceUnit.ROWS, all.getAllResults().size());
        getPipeTaskContext().getPipe().put(MaybeFinished.of(all));
    }
}
