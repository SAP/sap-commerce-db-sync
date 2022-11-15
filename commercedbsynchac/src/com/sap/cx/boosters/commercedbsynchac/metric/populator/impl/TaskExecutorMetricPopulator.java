/*
 *  Copyright: 2022 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsynchac.metric.populator.impl;

import com.sap.cx.boosters.commercedbsynchac.metric.populator.MetricPopulator;
import de.hybris.platform.commercedbsynchac.data.MetricData;
import org.apache.commons.lang.StringUtils;
import com.sap.cx.boosters.commercedbsync.context.MigrationContext;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

public class TaskExecutorMetricPopulator implements MetricPopulator {

    private AsyncTaskExecutor executor;
    private String name;

    public TaskExecutorMetricPopulator(AsyncTaskExecutor executor, String name) {
        this.executor = executor;
        this.name = name;
    }

    @Override
    public MetricData populate(MigrationContext context) throws Exception {
        if (!(executor instanceof ThreadPoolTaskExecutor)) {
            throw new RuntimeException("Populator can only be used for " + ThreadPoolTaskExecutor.class.getName());
        }
        ThreadPoolTaskExecutor taskExecutor = (ThreadPoolTaskExecutor) executor;
        MetricData data = new MetricData();
        int activeCount = taskExecutor.getActiveCount();
        int maxPoolSize = taskExecutor.getMaxPoolSize();
        data.setMetricId(name + "-executor");
        data.setName(StringUtils.capitalize(name) + " Tasks");
        data.setDescription("The tasks running in parallel in the task executor");
        data.setPrimaryValue((double) activeCount);
        data.setPrimaryValueLabel("Running");
        data.setPrimaryValueUnit("#");
        data.setPrimaryValueThreshold(-1d);
        data.setSecondaryValue((double) maxPoolSize - activeCount);
        data.setSecondaryValueLabel("Free");
        data.setSecondaryValueUnit("#");
        data.setSecondaryValueThreshold(-1d);
        populateColors(data);
        return data;
    }
}
