/*
 *  Copyright: 2023 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsynchac.metric.populator.impl;

import com.sap.cx.boosters.commercedbsync.repository.DataRepository;
import com.sap.cx.boosters.commercedbsync.repository.impl.AzureDataRepository;
import com.sap.cx.boosters.commercedbsynchac.metric.populator.MetricPopulator;
import de.hybris.platform.commercedbsynchac.data.MetricData;
import com.sap.cx.boosters.commercedbsync.context.MigrationContext;

import java.util.Optional;

public class DTUMetricPopulator implements MetricPopulator {
    @Override
    public MetricData populate(MigrationContext context) throws Exception {
        int primaryValue = getAzureDataRepository(context).map(DataRepository::getDatabaseUtilization).orElse(-1f)
                .intValue();
        if (primaryValue > 100) {
            primaryValue = 100;
        }
        int secondaryValue = 100 - primaryValue;
        if (primaryValue < 0) {
            primaryValue = -1;
            secondaryValue = -1;
        }

        MetricData data = new MetricData();

        data.setMetricId("dtu");
        data.setName("DTU");
        data.setDescription("The current DTU utilization of the azure database");
        data.setPrimaryValue((double) primaryValue);
        data.setPrimaryValueLabel("Used");
        data.setPrimaryValueUnit("%");
        data.setPrimaryValueThreshold(90d);
        data.setSecondaryValue((double) secondaryValue);
        data.setSecondaryValueLabel("Idle");
        data.setSecondaryValueUnit("%");
        data.setSecondaryValueThreshold(0d);
        populateColors(data);

        return data;
    }

    private Optional<DataRepository> getAzureDataRepository(MigrationContext context) {
        if (context.getDataTargetRepository() instanceof AzureDataRepository) {
            return Optional.of(context.getDataTargetRepository());
        } else if (context.getDataSourceRepository() instanceof AzureDataRepository) {
            return Optional.of(context.getDataSourceRepository());
        }

        return Optional.empty();
    }

    @Override
    public boolean canHandle(MigrationContext context) {
        return getAzureDataRepository(context).isPresent();
    }
}
