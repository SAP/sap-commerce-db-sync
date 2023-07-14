/*
 *  Copyright: 2023 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.processors.impl;

import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sap.cx.boosters.commercedbsync.context.CopyContext;
import com.sap.cx.boosters.commercedbsync.context.MigrationContext;
import com.sap.cx.boosters.commercedbsync.processors.MigrationPostProcessor;
import com.sap.cx.boosters.commercedbsync.repository.DataRepository;

public class ViewDropPostProcessor implements MigrationPostProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(ViewDropPostProcessor.class);

    private static final String DROP_VIEW = "DROP VIEW %s;";

    @Override
    public void process(final CopyContext context) {
        final MigrationContext migrationContext = context.getMigrationContext();
        final DataRepository dataSourceRepository = migrationContext.getDataSourceRepository();
        final Set<String> tables = migrationContext.getTablesForViews();

        tables.stream().forEach(table -> {
            try {
                final String viewName = migrationContext.getItemTypeViewNameByTable(table, dataSourceRepository);
                dataSourceRepository.executeUpdateAndCommitOnPrimary(String.format(DROP_VIEW, viewName));
                LOG.info("View {} is dropped", viewName);
            } catch (Exception e) {
                LOG.error("View dropped failed", e);
            }
        });
    }

    @Override
    public boolean shouldExecute(CopyContext context) {
        return context.getMigrationContext().isDataExportEnabled();
    }
}
