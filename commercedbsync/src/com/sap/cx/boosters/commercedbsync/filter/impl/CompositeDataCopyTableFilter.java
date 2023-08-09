/*
 *  Copyright: 2023 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.filter.impl;

import com.sap.cx.boosters.commercedbsync.context.MigrationContext;
import com.sap.cx.boosters.commercedbsync.filter.DataCopyTableFilter;

import java.util.List;
import java.util.function.Predicate;

public class CompositeDataCopyTableFilter implements DataCopyTableFilter {

    private List<DataCopyTableFilter> filters;

    @Override
    public Predicate<String> filter(MigrationContext context) {
        return p -> filters.stream().allMatch(f -> f.filter(context).test(p));
    }

    public void setFilters(List<DataCopyTableFilter> filters) {
        this.filters = filters;
    }
}
