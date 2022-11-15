/*
 *  Copyright: 2022 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.filter.impl;

import com.google.common.base.Predicates;
import com.sap.cx.boosters.commercedbsync.context.MigrationContext;
import com.sap.cx.boosters.commercedbsync.filter.DataCopyTableFilter;
import org.apache.commons.lang.StringUtils;

import java.util.Set;
import java.util.function.Predicate;

public class ExclusionDataCopyTableFilter implements DataCopyTableFilter {

    @Override
    public Predicate<String> filter(MigrationContext context) {
        Set<String> excludedTables = context.getExcludedTables();
        if (excludedTables == null || excludedTables.isEmpty()) {
            return Predicates.alwaysTrue();
        }
        return p -> excludedTables.stream().noneMatch(e -> StringUtils.equalsIgnoreCase(e, p));
    }
}
