/*
 *  Copyright: 2023 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsynchac.metric.populator.impl;

import com.sap.cx.boosters.commercedbsync.context.MigrationContext;

import javax.sql.DataSource;

public class HikariTargetConnectionMetricPopulator extends HikariConnectionMetricPopulator {

    @Override
    protected String getMetricId(MigrationContext context) {
        return "hikari-target-pool";
    }

    @Override
    protected String getName(MigrationContext context) {
        return "Target DB Pool";
    }

    @Override
    protected DataSource getDataSource(MigrationContext context) {
        return context.getDataTargetRepository().getDataSource();
    }
}
