/*
 *  Copyright: 2023 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.processors.impl;

import java.sql.SQLException;
import java.util.Objects;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sap.cx.boosters.commercedbsync.context.CopyContext;
import com.sap.cx.boosters.commercedbsync.context.MigrationContext;
import com.sap.cx.boosters.commercedbsync.processors.MigrationPreProcessor;
import com.sap.cx.boosters.commercedbsync.views.TableViewGenerator;

public class ViewGeneratorPreProcessor implements MigrationPreProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(ViewGeneratorPreProcessor.class);

    @Override
    public void process(final CopyContext context) {
        final MigrationContext ctx = context.getMigrationContext();
        final Set<String> tables = ctx.getTablesForViews();
        final TableViewGenerator generator = new TableViewGenerator();
        tables.stream().map(t -> {
            try {
                return generator.generateForTable(t, ctx);
            } catch (Exception e) {
                LOG.error(String.format("couldn't generate ctx for table %s", t), e);
                return null;
            }
        }).filter(Objects::nonNull).map(generator::generateViewDefinition).forEach(t -> {
            try {
                context.getMigrationContext().getDataSourceRepository().executeUpdateAndCommitOnPrimary(t);
            } catch (Exception e) {
                LOG.error(String.format("couldn't execute view creation %s", t), e);
            }
        });
        // Override setting if view has not been existing before
        context.getCopyItems().stream().forEach(ci -> {
            try {
                final String sTableName = context.getMigrationContext().getItemTypeViewNameByTable(ci.getSourceItem(),
                        context.getMigrationContext().getDataSourceRepository());
                ci.setSourceItem(sTableName);
            } catch (SQLException e) {
                LOG.error(String.format("could not check view mapping for table: %s", ci.getSourceItem()), e);
            }
        });
    }

    @Override
    public boolean shouldExecute(CopyContext context) {
        return context.getMigrationContext().isDataExportEnabled();
    }
}
