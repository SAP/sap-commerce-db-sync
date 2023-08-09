/*
 *  Copyright: 2023 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.adapter;

import com.sap.cx.boosters.commercedbsync.context.MigrationContext;
import com.sap.cx.boosters.commercedbsync.dataset.DataSet;
import com.sap.cx.boosters.commercedbsync.MarkersQueryDefinition;
import com.sap.cx.boosters.commercedbsync.OffsetQueryDefinition;
import com.sap.cx.boosters.commercedbsync.SeekQueryDefinition;

public interface DataRepositoryAdapter {
    long getRowCount(MigrationContext context, String table) throws Exception;

    DataSet getAll(MigrationContext context, String table) throws Exception;

    DataSet getBatchWithoutIdentifier(MigrationContext context, OffsetQueryDefinition queryDefinition) throws Exception;

    DataSet getBatchOrderedByColumn(MigrationContext context, SeekQueryDefinition queryDefinition) throws Exception;

    DataSet getBatchMarkersOrderedByColumn(MigrationContext context, MarkersQueryDefinition queryDefinition)
            throws Exception;

}
