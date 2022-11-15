/*
 *  Copyright: 2022 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.performance.impl;

import com.sap.cx.boosters.commercedbsync.performance.PerformanceCategory;
import com.sap.cx.boosters.commercedbsync.performance.PerformanceProfiler;
import com.sap.cx.boosters.commercedbsync.performance.PerformanceRecorder;
import com.sap.cx.boosters.commercedbsync.performance.PerformanceUnit;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class DefaultPerformanceProfiler implements PerformanceProfiler {

    private ConcurrentHashMap<String, PerformanceRecorder> recorders = new ConcurrentHashMap<>();


    @Override
    public PerformanceRecorder createRecorder(PerformanceCategory category, String name) {
        String recorderName = createRecorderName(category, name);
        return recorders.computeIfAbsent(recorderName, key -> new PerformanceRecorder(category, recorderName));
    }

    @Override
    public void muteRecorder(PerformanceCategory category, String name) {
        String recorderName = createRecorderName(category, name);
        this.recorders.remove(recorderName);
    }

    @Override
    public ConcurrentHashMap<String, PerformanceRecorder> getRecorders() {
        return recorders;
    }

    @Override
    public Collection<PerformanceRecorder> getRecordersByCategory(PerformanceCategory category) {
        return recorders.values().stream().filter(r -> category == r.getCategory()).collect(Collectors.toList());
    }

    @Override
    public double getAverageByCategoryAndUnit(PerformanceCategory category, PerformanceUnit unit) {
        Collection<PerformanceRecorder> recordersByCategory = getRecordersByCategory(category);
        return recordersByCategory.stream().filter(r -> r.getRecords().get(unit) != null).mapToDouble(r ->
                r.getRecords().get(unit).getAvgThroughput().get()
        ).average().orElse(0);
    }

    @Override
    public PerformanceRecorder getRecorder(PerformanceCategory category, String name) {
        return recorders.get(createRecorderName(category, name));
    }

    @Override
    public void reset() {
        getRecorders().clear();
    }

    protected String createRecorderName(PerformanceCategory category, String name) {
        return category + "->" + name;
    }

}
