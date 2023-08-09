/*
 *  Copyright: 2023 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsynchac.metric.populator.impl;

import com.sap.cx.boosters.commercedbsynchac.metric.populator.MetricPopulator;
import de.hybris.platform.commercedbsynchac.data.MetricData;
import com.sap.cx.boosters.commercedbsync.context.MigrationContext;
import org.springframework.beans.factory.annotation.Value;

import java.lang.management.OperatingSystemMXBean;

public class CpuMetricPopulator implements MetricPopulator {

    @Value("#{T(java.lang.management.ManagementFactory).getOperatingSystemMXBean()}")
    private OperatingSystemMXBean operatingSystemMXBean;

    @Override
    public MetricData populate(MigrationContext context) throws Exception {
        MetricData data = new MetricData();
        double systemLoadAverage = operatingSystemMXBean.getSystemLoadAverage();
        int availableProcessors = operatingSystemMXBean.getAvailableProcessors();
        int loadAverage = (int) (systemLoadAverage * 100 / availableProcessors);
        if (loadAverage > 100) {
            loadAverage = 100;
        }
        data.setMetricId("cpu");
        data.setName("CPU");
        data.setDescription("The system load in percent");
        data.setPrimaryValue((double) loadAverage);
        data.setPrimaryValueLabel("Load");
        data.setPrimaryValueUnit("%");
        data.setPrimaryValueThreshold(90d);
        data.setSecondaryValue((double) 100 - loadAverage);
        data.setSecondaryValueLabel("Idle");
        data.setSecondaryValueUnit("%");
        data.setSecondaryValueThreshold(0d);
        populateColors(data);
        return data;
    }
}
