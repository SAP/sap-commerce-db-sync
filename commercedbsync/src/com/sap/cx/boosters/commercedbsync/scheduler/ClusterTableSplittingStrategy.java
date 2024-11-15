/*
 *  Copyright: 2023 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.scheduler;

import com.sap.cx.boosters.commercedbsync.context.CopyContext;
import org.apache.commons.lang3.tuple.Pair;

import java.util.List;

public interface ClusterTableSplittingStrategy {
    List<Pair<CopyContext.DataCopyItem, Long>> split(CopyContext.DataCopyItem item, Long rowCount, int numNodes);
}
