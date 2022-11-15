/*
 *  Copyright: 2022 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsynchac.metric.populator.impl;

import com.sap.cx.boosters.commercedbsynchac.metric.populator.MetricPopulator;
import de.hybris.platform.commercedbsynchac.data.MetricData;
import com.sap.cx.boosters.commercedbsync.context.MigrationContext;

public class MemoryMetricPopulator implements MetricPopulator {
    @Override
    public MetricData populate(MigrationContext context) throws Exception {
        MetricData data = new MetricData();
        Runtime runtime = Runtime.getRuntime();
        double freeMemory = runtime.freeMemory() / 1048576L;
        double totalMemory = runtime.totalMemory() / 1048576L;
        double usedMemory = totalMemory - freeMemory;
        data.setMetricId("memory");
        data.setName("Memory");
        data.setDescription("The proportion of free and used memory");
        data.setPrimaryValue(usedMemory);
        data.setPrimaryValueLabel("Used");
        data.setPrimaryValueUnit("MB");
        data.setPrimaryValueThreshold(totalMemory * 0.9);
        data.setSecondaryValue(freeMemory);
        data.setSecondaryValueLabel("Free");
        data.setSecondaryValueUnit("MB");
        data.setSecondaryValueThreshold(0d);
        populateColors(data);
        return data;
    }
}
