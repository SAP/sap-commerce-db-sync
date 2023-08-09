/*
 *  Copyright: 2023 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.performance;

import java.util.Collection;
import java.util.concurrent.ConcurrentMap;

public interface PerformanceProfiler {
    PerformanceRecorder createRecorder(PerformanceCategory category, String name);

    void muteRecorder(PerformanceCategory category, String name);

    ConcurrentMap<String, PerformanceRecorder> getRecorders();

    Collection<PerformanceRecorder> getRecordersByCategory(PerformanceCategory category);

    double getAverageByCategoryAndUnit(PerformanceCategory category, PerformanceUnit unit);

    PerformanceRecorder getRecorder(PerformanceCategory category, String name);

    void reset();
}
