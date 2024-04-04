/*
 *  Copyright: 2023 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.concurrent.impl.task;

import com.sap.cx.boosters.commercedbsync.concurrent.PipeAbortedException;
import com.sap.cx.boosters.commercedbsync.constants.CommercedbsyncConstants;
import com.sap.cx.boosters.commercedbsync.context.CopyContext;
import com.sap.cx.boosters.commercedbsync.dataset.DataSet;
import de.hybris.platform.core.MasterTenant;
import org.openjdk.jol.info.GraphLayout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class DataReaderTask extends RetriableTask {

    private static final Logger LOG = LoggerFactory.getLogger(DataReaderTask.class);

    private final PipeTaskContext pipeTaskContext;

    protected DataReaderTask(PipeTaskContext pipeTaskContext) {
        super(pipeTaskContext.getContext(), pipeTaskContext.getTable());
        this.pipeTaskContext = pipeTaskContext;
    }

    public PipeTaskContext getPipeTaskContext() {
        return pipeTaskContext;
    }

    protected void waitForFreeMemory() throws Exception {
        CopyContext context = getPipeTaskContext().getContext();
        final long minMem = context.getMigrationContext().getMemoryMin();
        long freeMem = Runtime.getRuntime().freeMemory();

        int cnt = 0;
        while (freeMem < minMem) {
            LOG.trace("Waiting for freeMem  {} / {} Attempts={}", freeMem, minMem, cnt);
            Thread.sleep(context.getMigrationContext().getMemoryWait());
            cnt++;
            if (cnt >= context.getMigrationContext().getMemoryMaxAttempts()) {
                throw new PipeAbortedException("Maximum wait time exceeded. See property "
                        + CommercedbsyncConstants.MIGRATION_PROFILING_MEMORY_ATTEMPTS + " for more details.");
            }
            freeMem = Runtime.getRuntime().freeMemory();
        }
    }

    protected void profileData(final CopyContext context, final int batchId, final String table, final long pageSize,
            final DataSet result) {
        if (context.getMigrationContext().isProfiling() && result != null) {
            final long objSize = GraphLayout.parseInstance(result.getAllResults()).totalSize();
            final long freeMem = Runtime.getRuntime().freeMemory();
            final int clusterID = MasterTenant.getInstance().getClusterID();
            LOG.trace(
                    "Memory usage: [{}], Table = {}, BatchId = {}, Page Size = {}, Batch Memory Size = {}, Free System Memory = {}",
                    clusterID, table, batchId, pageSize, objSize, freeMem);
        }
    }
}
