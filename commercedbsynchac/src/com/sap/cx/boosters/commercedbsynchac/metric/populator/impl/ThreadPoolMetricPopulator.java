/*
 *  Copyright: 2023 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsynchac.metric.populator.impl;

import com.sap.cx.boosters.commercedbsync.concurrent.DataThreadPoolFactory;
import com.sap.cx.boosters.commercedbsync.context.MigrationContext;
import com.sap.cx.boosters.commercedbsynchac.metric.populator.MetricPopulator;
import de.hybris.platform.commercedbsynchac.data.MetricData;
import org.apache.commons.lang.StringUtils;

public class ThreadPoolMetricPopulator implements MetricPopulator {

    private final DataThreadPoolFactory factory;
    private final String name;

    public ThreadPoolMetricPopulator(DataThreadPoolFactory factory, String name) {
        this.factory = factory;
        this.name = name;
    }

    @Override
    public MetricData populate(MigrationContext context) throws Exception {
        MetricData data = new MetricData();
        double activeCount = factory.getMonitor().getActiveCount();
        double maxPoolSize = factory.getMonitor().getMaxPoolSize();
        if (maxPoolSize < 1) {
            // make primary and secondary value negative to indicate inactive widget
            activeCount = -1;
            maxPoolSize = -2;
        }
        data.setMetricId(name + "-executor");
        data.setName(StringUtils.capitalize(name) + " Tasks");
        data.setDescription("The workers running in parallel in the task executor");
        data.setPrimaryValue(activeCount);
        data.setPrimaryValueLabel("Running");
        data.setPrimaryValueUnit("#");
        data.setPrimaryValueThreshold(-1d);
        data.setSecondaryValue(maxPoolSize - activeCount);
        data.setSecondaryValueLabel("Free");
        data.setSecondaryValueUnit("#");
        data.setSecondaryValueThreshold(-1d);
        populateColors(data);
        return data;
    }
}
