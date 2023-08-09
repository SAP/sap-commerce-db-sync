/*
 *  Copyright: 2023 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.scheduler;

import java.util.List;

public interface DatabaseCopySchedulerAlgorithm {
    int getOwnNodeId();

    List<Integer> getNodeIds();

    int next();

    void reset();
}
