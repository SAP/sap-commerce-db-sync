/*
 *  Copyright: 2022 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsynchac.metric.populator;

import de.hybris.platform.commercedbsynchac.data.MetricData;
import com.sap.cx.boosters.commercedbsync.context.MigrationContext;

public interface MetricPopulator {
    static String PRIMARY_STANDARD_COLOR = "#92cae4";
    static String PRIMARY_CRITICAL_COLOR = "#de5d70";
    static String SECONDARY_STANDARD_COLOR = "#d5edf8";
    static String SECONDARY_CRITICAL_COLOR = "#e8acb5";

    MetricData populate(MigrationContext context) throws Exception;

    default void populateColors(MetricData data) {
        data.setPrimaryValueStandardColor(PRIMARY_STANDARD_COLOR);
        data.setPrimaryValueCriticalColor(PRIMARY_CRITICAL_COLOR);
        data.setSecondaryValueStandardColor(SECONDARY_STANDARD_COLOR);
        data.setSecondaryValueCriticalColor(SECONDARY_CRITICAL_COLOR);
    }
}
