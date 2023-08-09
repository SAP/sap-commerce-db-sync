/*
 *  Copyright: 2023 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.adapter.impl;

import com.sap.cx.boosters.commercedbsync.adapter.DataRepositoryAdapter;
import com.sap.cx.boosters.commercedbsync.constants.CommercedbsyncConstants;
import com.sap.cx.boosters.commercedbsync.context.MigrationContext;
import com.sap.cx.boosters.commercedbsync.dataset.DataSet;
import com.sap.cx.boosters.commercedbsync.repository.DataRepository;
import com.sap.cx.boosters.commercedbsync.MarkersQueryDefinition;
import com.sap.cx.boosters.commercedbsync.OffsetQueryDefinition;
import com.sap.cx.boosters.commercedbsync.SeekQueryDefinition;

import java.time.Instant;

/**
 * Controls the way the repository is accessed by adapting the most common
 * reading operations based on the configured context
 */
public class ContextualDataRepositoryAdapter implements DataRepositoryAdapter {

    private final DataRepository repository;

    public ContextualDataRepositoryAdapter(DataRepository repository) {
        this.repository = repository;
    }

    @Override
    public long getRowCount(MigrationContext context, String table) throws Exception {
        if (context.isDeletionEnabled() || context.isLpTableMigrationEnabled()) {
            return repository.getRowCountModifiedAfter(table, getIncrementalTimestamp(context),
                    context.isDeletionEnabled(), context.isLpTableMigrationEnabled());
        } else {
            if (context.isIncrementalModeEnabled()) {
                return repository.getRowCountModifiedAfter(table, getIncrementalTimestamp(context));
            } else {
                return repository.getRowCount(table);
            }
        }
    }

    @Override
    public DataSet getAll(MigrationContext context, String table) throws Exception {
        if (context.isIncrementalModeEnabled()) {
            return repository.getAllModifiedAfter(table, getIncrementalTimestamp(context));
        } else {
            return repository.getAll(table);
        }
    }

    @Override
    public DataSet getBatchWithoutIdentifier(MigrationContext context, OffsetQueryDefinition queryDefinition)
            throws Exception {
        if (context.isIncrementalModeEnabled()) {
            return repository.getBatchWithoutIdentifier(queryDefinition, getIncrementalTimestamp(context));
        } else {
            return repository.getBatchWithoutIdentifier(queryDefinition);
        }
    }

    @Override
    public DataSet getBatchOrderedByColumn(MigrationContext context, SeekQueryDefinition queryDefinition)
            throws Exception {
        if (context.isIncrementalModeEnabled()) {
            return repository.getBatchOrderedByColumn(queryDefinition, getIncrementalTimestamp(context));
        } else {
            return repository.getBatchOrderedByColumn(queryDefinition);
        }
    }

    @Override
    public DataSet getBatchMarkersOrderedByColumn(MigrationContext context, MarkersQueryDefinition queryDefinition)
            throws Exception {
        if (context.isIncrementalModeEnabled()) {
            return repository.getBatchMarkersOrderedByColumn(queryDefinition, getIncrementalTimestamp(context));
        } else {
            return repository.getBatchMarkersOrderedByColumn(queryDefinition);
        }
    }

    private Instant getIncrementalTimestamp(MigrationContext context) {
        Instant incrementalTimestamp = context.getIncrementalTimestamp();
        if (incrementalTimestamp == null) {
            throw new IllegalStateException(
                    "Timestamp cannot be null in incremental mode. Set a timestamp using the property "
                            + CommercedbsyncConstants.MIGRATION_DATA_INCREMENTAL_TIMESTAMP);
        }
        return incrementalTimestamp;
    }
}
