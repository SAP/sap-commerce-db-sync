/*
 *  Copyright: 2023 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.filter.impl;

import com.google.common.base.Predicates;
import com.sap.cx.boosters.commercedbsync.constants.CommercedbsyncConstants;
import com.sap.cx.boosters.commercedbsync.context.MigrationContext;
import com.sap.cx.boosters.commercedbsync.filter.DataCopyTableFilter;
import org.apache.commons.lang.StringUtils;

import java.util.Set;
import java.util.function.Predicate;

public class IncrementalDataCopyTableFilter implements DataCopyTableFilter {

    @Override
    public Predicate<String> filter(MigrationContext context) {
        if (!context.isIncrementalModeEnabled()) {
            return Predicates.alwaysTrue();
        }
        Set<String> incrementalTables = context.getIncrementalTables();
        if (incrementalTables == null || incrementalTables.isEmpty()) {
            throw new IllegalStateException("At least one table for incremental copy must be specified. Check property "
                    + CommercedbsyncConstants.MIGRATION_DATA_INCREMENTAL_TABLES);
        }
        return p -> incrementalTables.stream().anyMatch(e -> StringUtils.equalsIgnoreCase(e, p));
    }
}
