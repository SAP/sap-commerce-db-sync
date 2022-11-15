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

public class InclusionDataCopyTableFilter implements DataCopyTableFilter {

    @Override
    public Predicate<String> filter(MigrationContext context) {
        Set<String> includedTables = context.getIncludedTables();
        if (includedTables == null || includedTables.isEmpty()) {
            return Predicates.alwaysTrue();
        }
        return p -> includedTables.stream().anyMatch(e -> StringUtils.equalsIgnoreCase(e, p));

    }
}
