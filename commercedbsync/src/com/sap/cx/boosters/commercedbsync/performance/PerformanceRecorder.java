/*
 *  Copyright: 2023 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.performance;

import com.google.common.base.Joiner;
import com.google.common.base.Stopwatch;
import com.google.common.util.concurrent.AtomicDouble;

import javax.annotation.concurrent.ThreadSafe;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class PerformanceRecorder {

    private final ConcurrentMap<PerformanceUnit, PerformanceAggregation> records = new ConcurrentHashMap<>();

    private final Stopwatch timer;
    private final PerformanceCategory category;
    private final String name;

    public PerformanceRecorder(PerformanceCategory category, String name) {
        this(category, name, false);
    }

    public PerformanceRecorder(PerformanceCategory category, String name, boolean autoStart) {
        this.category = category;
        this.name = name;
        if (autoStart) {
            this.timer = Stopwatch.createStarted();
        } else {
            this.timer = Stopwatch.createUnstarted();
        }
    }

    public void start() {
        if (!this.timer.isRunning()) {
            this.timer.start();
        }
    }

    public void pause() {
        this.timer.stop();
    }

    public String getName() {
        return name;
    }

    public PerformanceCategory getCategory() {
        return category;
    }

    public void record(PerformanceUnit unit, double value) {
        if (getRecords().containsKey(unit)) {
            getRecords().get(unit).submit(value);
        } else {
            PerformanceAggregation performanceAggregation = new PerformanceAggregation(getTimer(), unit);
            performanceAggregation.submit(value);
            getRecords().put(unit, performanceAggregation);
        }
    }

    public ConcurrentMap<PerformanceUnit, PerformanceAggregation> getRecords() {
        return records;
    }

    private Stopwatch getTimer() {
        return timer;
    }

    @Override
    public String toString() {
        return "PerformanceRecorder{name=" + getName() + ",{" + Joiner.on("},{").join(getRecords().values()) + "}}";
    }

    @ThreadSafe
    public static class PerformanceAggregation {

        private final Stopwatch timer;
        private final PerformanceUnit performanceUnit;
        private final TimeUnit timeUnit = TimeUnit.SECONDS;
        private final AtomicDouble sum = new AtomicDouble(0);
        private final AtomicDouble max = new AtomicDouble(0);
        private final AtomicDouble min = new AtomicDouble(0);
        private final AtomicDouble avg = new AtomicDouble(0);

        public PerformanceAggregation(Stopwatch timer, PerformanceUnit performanceUnit) {
            this.performanceUnit = performanceUnit;
            this.timer = timer;
        }

        protected void submit(double value) {
            getTotalThroughput().addAndGet(value);
            long elapsed = timer.elapsed(TimeUnit.MILLISECONDS);
            double elapsedToSeconds = elapsed / 1000d;
            if (elapsedToSeconds > 0) {
                getAvgThroughput().set(getTotalThroughput().get() / elapsedToSeconds);
                getMaxThroughput().set(Math.max(getMaxThroughput().get(), getAvgThroughput().get()));
                getMinThroughput().set(Math.max(getMinThroughput().get(), getAvgThroughput().get()));
            }
        }

        public PerformanceUnit getPerformanceUnit() {
            return performanceUnit;
        }

        public AtomicDouble getTotalThroughput() {
            return sum;
        }

        public AtomicDouble getAvgThroughput() {
            return avg;
        }

        public AtomicDouble getMinThroughput() {
            return min;
        }

        public AtomicDouble getMaxThroughput() {
            return max;
        }

        public TimeUnit getTimeUnit() {
            return timeUnit;
        }

        @Override
        public String toString() {
            return "PerformanceAggregation{" + "performanceUnit=" + performanceUnit + ", sum=" + sum + ", max=" + max
                    + " " + performanceUnit + "/" + timeUnit + ", min=" + min + " " + performanceUnit + "/" + timeUnit
                    + ", avg=" + avg + " " + performanceUnit + "/" + timeUnit + '}';
        }
    }
}
