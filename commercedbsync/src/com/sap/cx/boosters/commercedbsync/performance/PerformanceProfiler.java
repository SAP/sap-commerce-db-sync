/*
 *  Copyright: 2026 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.performance;

import com.sap.cx.boosters.commercedbsync.context.CopyContext;

import java.util.Collection;
import java.util.concurrent.ConcurrentMap;

public interface PerformanceProfiler {
    PerformanceRecorder createRecorder(PerformanceCategory category, String name);

    default PerformanceRecorder createRecorder(PerformanceCategory category, CopyContext.DataCopyItem copyItem) {
        return createRecorder(category, PerformanceRecorder.getRecorderName(copyItem, category));
    }

    void muteRecorder(PerformanceCategory category, String name);

    default void muteRecorder(PerformanceCategory category, CopyContext.DataCopyItem copyItem) {
        muteRecorder(category, PerformanceRecorder.getRecorderName(copyItem, category));
    }

    ConcurrentMap<String, PerformanceRecorder> getRecorders();

    Collection<PerformanceRecorder> getRecordersByCategory(PerformanceCategory category);

    double getAverageByCategoryAndUnit(PerformanceCategory category, PerformanceUnit unit);

    PerformanceRecorder getRecorder(PerformanceCategory category, String name);

    default PerformanceRecorder getRecorder(PerformanceCategory category, CopyContext.DataCopyItem copyItem) {
        return getRecorder(category, PerformanceRecorder.getRecorderName(copyItem, category));
    }

    void reset();
}
