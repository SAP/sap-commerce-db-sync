/*
 *  Copyright: 2023 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsynchac.metric.impl;

import de.hybris.platform.commercedbsynchac.data.MetricData;
import com.sap.cx.boosters.commercedbsync.context.MigrationContext;
import com.sap.cx.boosters.commercedbsynchac.metric.MetricService;
import com.sap.cx.boosters.commercedbsynchac.metric.populator.MetricPopulator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class DefaultMetricService implements MetricService {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultMetricService.class);

    private final List<MetricPopulator> populators;

    public DefaultMetricService(List<MetricPopulator> populators) {
        this.populators = populators;
    }

    @Override
    public List<MetricData> getMetrics(MigrationContext context) {
        List<MetricData> dataList = new ArrayList<>();
        for (MetricPopulator populator : populators) {
            try {
                dataList.add(populator.populate(context));
            } catch (Exception e) {
                LOG.error("Error while populating metric. Populator: " + e.getMessage());
            }
        }
        return dataList;
    }

}
