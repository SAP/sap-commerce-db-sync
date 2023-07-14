/*
 *  Copyright: 2023 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsynchac.metric.populator.impl;

import de.hybris.platform.commercedbsynchac.data.MetricData;
import com.sap.cx.boosters.commercedbsync.context.MigrationContext;
import com.sap.cx.boosters.commercedbsync.performance.PerformanceCategory;
import com.sap.cx.boosters.commercedbsync.performance.PerformanceProfiler;
import com.sap.cx.boosters.commercedbsync.performance.PerformanceUnit;
import com.sap.cx.boosters.commercedbsynchac.metric.populator.MetricPopulator;

public class IOMetricPopulator implements MetricPopulator {

    private final PerformanceProfiler performanceProfiler;

    public IOMetricPopulator(PerformanceProfiler performanceProfiler) {
        this.performanceProfiler = performanceProfiler;
    }

    @Override
    public MetricData populate(MigrationContext context) throws Exception {
        MetricData data = new MetricData();
        int avgRowReading = (int) performanceProfiler.getAverageByCategoryAndUnit(PerformanceCategory.DB_READ,
                PerformanceUnit.ROWS);
        int avgRowWriting = (int) performanceProfiler.getAverageByCategoryAndUnit(PerformanceCategory.DB_WRITE,
                PerformanceUnit.ROWS);
        int totalIO = avgRowReading + avgRowWriting;
        if (avgRowReading < 1 && avgRowWriting < 1) {
            avgRowReading = -1;
            avgRowWriting = -1;
        }
        data.setMetricId("db-io");
        data.setName("Database I/O");
        data.setDescription("The proportion of items read from source compared to items written to target");
        data.setPrimaryValue((double) avgRowReading);
        data.setPrimaryValueLabel("Read");
        data.setPrimaryValueUnit("rows/s");
        data.setPrimaryValueThreshold(totalIO * 0.75);
        data.setSecondaryValue((double) avgRowWriting);
        data.setSecondaryValueLabel("Write");
        data.setSecondaryValueUnit("rows/s");
        data.setSecondaryValueThreshold(totalIO * 0.75);
        populateColors(data);
        return data;
    }
}
