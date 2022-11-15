/*
 *  Copyright: 2022 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.filter;

import com.sap.cx.boosters.commercedbsync.context.MigrationContext;

import java.util.function.Predicate;

public interface DataCopyTableFilter {
    Predicate<String> filter(MigrationContext context);
}
