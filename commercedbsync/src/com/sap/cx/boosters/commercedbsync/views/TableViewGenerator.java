/*
 *  Copyright: 2023 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.views;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.text.StringSubstitutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;
import com.sap.cx.boosters.commercedbsync.context.MigrationContext;

public class TableViewGenerator {

    private static final String VIEW_PREFIX = "CREATE OR ALTER VIEW ${name} AS SELECT ${columns} FROM ${from}";
    private static final String COLUMN_REPLACEMENT = "${replacement} AS ${column}";

    private static final Logger LOG = LoggerFactory.getLogger(TableViewGenerator.class);

    /**
     * Generates {@link ViewConfigurationContext} context with all required data for
     * <code>VIEW</code> DDL generation.
     *
     * @param tableName
     *            raw table name which should be a source of data
     * @param migrationContext
     *            holds all information about source data
     * @return
     * @throws Exception
     */
    public ViewConfigurationContext generateForTable(final String tableName, final MigrationContext migrationContext)
            throws Exception {
        try {
            Set<String> columns = migrationContext.getDataSourceRepository().getAllColumnNames(tableName);
            if (columns.isEmpty()) {
                return null;
            }
            String viewPrefix = migrationContext.getItemTypeViewNamePattern();
            String tableVieName = String.format(StringUtils.trimToEmpty(viewPrefix), tableName);
            String whereView = migrationContext.getViewWhereClause(tableName);
            Map<String, String> customColumns = migrationContext.getCustomColumnsForView(tableName);
            final String viewColumnPrefix = migrationContext.getViewColumnPrefixFor(tableName);
            return new ViewConfigurationContext(tableName, tableVieName, columns, customColumns, whereView,
                    viewColumnPrefix);
        } catch (Exception e) {
            LOG.error(String.format("could not get source data repository for table: %s", tableName), e);
            throw e;
        }
    }

    /**
     * generates DDL VIEW definition based on ctx for given table. Template of that
     * string is:<br/>
     * <code>CREATE OR ALTER VIEW ${name} AS SELECT ${columns} FROM ${from}</code>
     * <p>
     * where:
     *
     * <li>
     * <ul>
     * name - view name
     * </ul>
     * <ul>
     * columns - view columns with optional custom column definition
     * </ul>
     * <ul>
     * from - from source table, or optional custom DQL <code>FROM</code> section
     * </ul>
     * </li>
     *
     * @param ctx
     * @return
     */
    public String generateViewDefinition(final ViewConfigurationContext ctx) {
        String columnList = generateColumnList(ctx);
        Map<String, String> params = Map.of("name", ctx.getView(), "columns", columnList, "from",
                ctx.getAdditionalWhereClause());
        StringSubstitutor template = new StringSubstitutor(params);
        String view = template.replace(VIEW_PREFIX);
        LOG.debug(String.format("generated view for table %s: %s", ctx.getTable(), view));
        return view;
    }

    /**
     * method generates from context list of columns, where if there is custom
     * column definition it will replace original one
     *
     * @param ctx
     * @return comma separated list of columns for view built from ctx
     */
    public String generateColumnList(final ViewConfigurationContext ctx) {
        Set<String> originalColumns = ctx.getOriginalColumns();
        Map<String, String> replacements = ctx.getColumnReplacements();
        Set<String> newColumnSet = originalColumns.stream().collect(Collectors.toSet());

        final String viewColumnPrefix = ctx.getViewColumnPrefix();
        if (StringUtils.isNotBlank(viewColumnPrefix)) {
            newColumnSet = newColumnSet.stream().map(column -> viewColumnPrefix + "." + column)
                    .collect(Collectors.toSet());
        }

        for (Entry<String, String> entry : replacements.entrySet()) {
            String replacementKey = entry.getKey();
            if (StringUtils.isNotBlank(viewColumnPrefix)) {
                replacementKey = viewColumnPrefix + "." + replacementKey;
            }

            if (!newColumnSet.contains(replacementKey)) {
                LOG.warn(String.format("There is missing column %s in table %s, for custom definition %s",
                        entry.getKey(), ctx.getTable(), entry.getValue()));
            } else {
                Map<String, String> columnMap = Map.of("replacement", entry.getValue(), "column", entry.getKey());
                StringSubstitutor t = new StringSubstitutor(columnMap);
                String replacement = t.replace(COLUMN_REPLACEMENT);
                // replace old one with new
                newColumnSet.remove(replacementKey);
                newColumnSet.add(replacement);
            }
        }

        return Joiner.on(", ").join(newColumnSet);
    }

    public static String getTableNameForView(final String tableName, final MigrationContext migrationContext) {
        final String viewPrefix = String.format(StringUtils.trimToEmpty(migrationContext.getItemTypeViewNamePattern()),
                "");
        if (tableName.startsWith(viewPrefix)) {
            return tableName.replace(viewPrefix, "");
        }
        return tableName;
    }
}
