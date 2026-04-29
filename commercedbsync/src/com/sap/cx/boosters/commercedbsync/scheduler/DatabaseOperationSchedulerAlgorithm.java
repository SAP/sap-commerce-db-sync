/*
 *  Copyright: 2026 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.scheduler;

import java.util.List;

public interface DatabaseOperationSchedulerAlgorithm {
    int getOwnNodeId();

    List<Integer> getNodeIds(String tableName);

    int next(String tableName);

    void reset();
}
