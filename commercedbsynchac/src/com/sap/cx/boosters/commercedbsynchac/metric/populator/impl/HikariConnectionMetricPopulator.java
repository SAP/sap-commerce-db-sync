/*
 *  Copyright: 2022 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsynchac.metric.populator.impl;

import com.sap.cx.boosters.commercedbsynchac.metric.populator.MetricPopulator;
import com.zaxxer.hikari.HikariDataSource;
import de.hybris.platform.commercedbsynchac.data.MetricData;
import com.sap.cx.boosters.commercedbsync.context.MigrationContext;

import javax.sql.DataSource;

public abstract class HikariConnectionMetricPopulator implements MetricPopulator {

    @Override
    public MetricData populate(MigrationContext context) throws Exception {
        if (!(getDataSource(context) instanceof HikariDataSource)) {
            throw new RuntimeException("Populator cannot be used for non-hikari datasources");
        }
        MetricData data = new MetricData();
        HikariDataSource hikariDS = (HikariDataSource) getDataSource(context);
        double activeConnections = hikariDS.getHikariPoolMXBean().getActiveConnections();
        double maxConnections = hikariDS.getHikariConfigMXBean().getMaximumPoolSize();
        data.setMetricId(getMetricId(context));
        data.setName(getName(context));
        data.setDescription("The proportion of active and idle hikari connections");
        data.setPrimaryValue(activeConnections);
        data.setPrimaryValueLabel("Active");
        data.setPrimaryValueUnit("#");
        data.setPrimaryValueThreshold((double) maxConnections);
        data.setSecondaryValue(maxConnections - activeConnections);
        data.setSecondaryValueLabel("Idle");
        data.setSecondaryValueUnit("#");
        data.setSecondaryValueThreshold(0d);
        populateColors(data);
        return data;
    }

    protected abstract String getMetricId(MigrationContext context);

    protected abstract String getName(MigrationContext context);

    protected abstract DataSource getDataSource(MigrationContext context);
}
