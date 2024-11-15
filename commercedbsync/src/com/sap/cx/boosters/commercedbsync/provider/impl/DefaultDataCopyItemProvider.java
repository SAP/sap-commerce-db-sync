/*
 *  Copyright: 2023 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.provider.impl;

import com.google.common.collect.Sets;
import com.sap.cx.boosters.commercedbsync.constants.CommercedbsyncConstants;
import com.sap.cx.boosters.commercedbsync.provider.CopyItemProvider;
import de.hybris.bootstrap.ddl.DataBaseProvider;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import com.sap.cx.boosters.commercedbsync.TableCandidate;
import com.sap.cx.boosters.commercedbsync.TypeSystemTable;
import com.sap.cx.boosters.commercedbsync.context.CopyContext;
import com.sap.cx.boosters.commercedbsync.context.MigrationContext;
import com.sap.cx.boosters.commercedbsync.filter.DataCopyTableFilter;
import com.sap.cx.boosters.commercedbsync.repository.DataRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

public class DefaultDataCopyItemProvider implements CopyItemProvider {

    public static final String SN_SUFFIX = "sn";
    private static final String LP_SUFFIX = "lp";
    private static final String LP_SUFFIX_UPPER = "LP";

    private static final Logger LOG = LoggerFactory.getLogger(DefaultDataCopyItemProvider.class);

    private final Comparator<TableCandidate> tableCandidateComparator = (o1, o2) -> o1.getCommonTableName()
            .compareToIgnoreCase(o2.getCommonTableName());
    private DataCopyTableFilter dataCopyTableFilter;

    private static boolean shouldMigrateAuditTable(final MigrationContext context, final String auditTableName) {
        return context.isAuditTableMigrationEnabled() && StringUtils.isNotEmpty(auditTableName);
    }

    private void logTables(final Set<TableCandidate> tablesCandidates, final String debugtext) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("---------START------," + debugtext);
            if (tablesCandidates != null && tablesCandidates.size() > 0) {
                for (final TableCandidate source : tablesCandidates) {
                    LOG.debug("$$Table Common Name = " + source.getCommonTableName() + ", Base Table = "
                            + source.getBaseTableName() + ", Suffix = " + source.getAdditionalSuffix()
                            + ", Table Name = " + source.getFullTableName());
                }
                LOG.debug("---------END------," + debugtext);
            }
        }

    }

    @Override
    public Set<CopyContext.DataCopyItem> get(final MigrationContext context) throws Exception {
        final Set<TableCandidate> sourceTablesCandidates = getSourceTableCandidates(context);
        final Set<TableCandidate> targetTablesCandidates = getTargetTableCandidates(context);
        final Sets.SetView<TableCandidate> sourceTables = Sets.intersection(sourceTablesCandidates,
                targetTablesCandidates);

        logTables(sourceTablesCandidates, "source table candidates");
        logTables(targetTablesCandidates, "target table candidates");
        logTables(sourceTables, "intersection tables");

        final Set<TableCandidate> sourceTablesToMigrate = sourceTables.stream()
                .filter(t -> dataCopyTableFilter.filter(context).test(t.getCommonTableName()))
                .collect(Collectors.toSet());

        return createCopyItems(context, sourceTablesToMigrate, targetTablesCandidates.stream()
                .collect(Collectors.toMap(t -> t.getCommonTableName().toLowerCase(), t -> t)));
    }

    @Override
    public Set<TableCandidate> getSourceTableCandidates(final MigrationContext context) throws Exception {
        return getTableCandidates(context, context.getDataSourceRepository());
    }

    @Override
    public Set<TableCandidate> getTargetTableCandidates(final MigrationContext context) throws Exception {
        final DataRepository targetRepository = context.getDataTargetRepository();
        final String prefix = targetRepository.getDataSourceConfiguration().getTablePrefix();

        return targetRepository.getAllTableNames().stream()
                .filter(n -> prefix == null || StringUtils.startsWithIgnoreCase(n, prefix))
                .filter(n -> !isNonMatchingTypesystemTable(targetRepository,
                        StringUtils.removeStartIgnoreCase(n, prefix)))
                .map(n -> createTableCandidate(targetRepository, n))
                .collect(Collectors.toCollection(() -> new TreeSet<>(tableCandidateComparator)));
    }

    private boolean isNonMatchingTypesystemTable(final DataRepository repository, final String tableName) {
        boolean isTypesystemTable = false;

        if (!StringUtils.endsWithIgnoreCase(tableName, SN_SUFFIX)) {
            isTypesystemTable = Arrays.stream(TYPE_SYSTEM_RELATED_TYPES)
                    .anyMatch(t -> StringUtils.startsWithIgnoreCase(tableName, t));
        }
        if (isTypesystemTable) {

            final String additionalSuffix = getAdditionalSuffix(tableName, repository.getDatabaseProvider());
            final String tableNameWithoutAdditionalSuffix = getTableNameWithoutAdditionalSuffix(tableName,
                    additionalSuffix);
            final String typeSystemSuffix = repository.getDataSourceConfiguration().getTypeSystemSuffix();
            LOG.debug("$$TS table name=" + tableName + ",additionalSuffix=" + additionalSuffix
                    + ", tableNameWithoutAdditionalSuffix=" + tableNameWithoutAdditionalSuffix + ",typeSystemSuffix="
                    + typeSystemSuffix);
            LOG.debug("$$TS check="
                    + !StringUtils.endsWithIgnoreCase(tableNameWithoutAdditionalSuffix, typeSystemSuffix));
            return !StringUtils.endsWithIgnoreCase(tableNameWithoutAdditionalSuffix, typeSystemSuffix);
        }
        return false;
    }

    private Set<TableCandidate> getTableCandidates(final MigrationContext context, final DataRepository repository)
            throws Exception {
        final Set<String> allTableNames = repository.getAllTableNames();

        LOG.debug("$$ALL TABLES...getTableCandidates " + allTableNames);
        final Set<TableCandidate> tableCandidates = new TreeSet<>(tableCandidateComparator);
        final String tablePrefix = StringUtils.trimToEmpty(repository.getDataSourceConfiguration().getTablePrefix());

        // add meta tables
        tableCandidates.add(createTableCandidate(repository, CommercedbsyncConstants.DEPLOYMENTS_TABLE, tablePrefix));
        tableCandidates.add(createTableCandidate(repository, "aclentries", tablePrefix));
        tableCandidates.add(createTableCandidate(repository, "configitems", tablePrefix));
        tableCandidates.add(createTableCandidate(repository, "numberseries", tablePrefix));
        tableCandidates.add(createTableCandidate(repository, "metainformations", tablePrefix));

        // add tables listed in "ydeployments"
        final Set<TypeSystemTable> allTypeSystemTables = repository.getAllTypeSystemTables();
        allTypeSystemTables.forEach(t -> {
            tableCandidates.add(createTableCandidate(repository, t.getTableName(), tablePrefix));

            final String propsTableName = t.getPropsTableName();

            if (StringUtils.isNotEmpty(propsTableName)) {
                tableCandidates.add(createTableCandidate(repository, t.getPropsTableName(), tablePrefix));
            }

            final TableCandidate lpTable = createTableCandidate(repository, t.getTableName() + LP_SUFFIX, tablePrefix);

            if (allTableNames.stream().anyMatch(lpTable.getFullTableName()::equalsIgnoreCase)) {
                LOG.debug("LP table Match... " + lpTable.getFullTableName());
                tableCandidates.add(lpTable);
            }

            if (shouldMigrateAuditTable(context, t.getAuditTableName())) {
                final TableCandidate auditTable = createTableCandidate(repository, t.getAuditTableName(), tablePrefix);

                if (allTableNames.contains(auditTable.getFullTableName())) {
                    tableCandidates.add(auditTable);
                }
            }
        });

        // custom tables
        if (CollectionUtils.isNotEmpty(context.getCustomTables())) {
            tableCandidates.addAll(context.getCustomTables().stream().map(t -> createTableCandidate(repository, t))
                    .collect(Collectors.toSet()));
        }

        return tableCandidates;
    }

    private TableCandidate createTableCandidate(final DataRepository repository, final String tableName,
            String tableNamePrefix) {
        return createTableCandidate(repository, tableNamePrefix + tableName);
    }

    private TableCandidate createTableCandidate(final DataRepository repository, final String tableName) {
        final TableCandidate candidate = new TableCandidate();

        final String additionalSuffix = getAdditionalSuffix(tableName, repository.getDatabaseProvider());
        final String tableNameWithoutAdditionalSuffix = getTableNameWithoutAdditionalSuffix(tableName,
                additionalSuffix);

        String baseTableName = getTableNameWithoutPrefix(tableNameWithoutAdditionalSuffix,
                repository.getDataSourceConfiguration().getTablePrefix());
        final String typeSystemSuffix = repository.getDataSourceConfiguration().getTypeSystemSuffix();
        final boolean isTypeSystemRelatedTable = isTypeSystemRelatedTable(baseTableName, typeSystemSuffix);

        if (isTypeSystemRelatedTable) {
            baseTableName = getTableNameWithoutTypeSystemSuffix(baseTableName, typeSystemSuffix);
        }

        candidate.setCommonTableName(baseTableName + additionalSuffix);
        candidate.setFullTableName(tableName);
        candidate.setAdditionalSuffix(additionalSuffix);
        candidate.setBaseTableName(baseTableName);
        candidate.setTypeSystemRelatedTable(isTypeSystemRelatedTable);
        return candidate;
    }

    private boolean isTypeSystemRelatedTable(final String tableName, final String typeSystemSuffix) {
        return Arrays.stream(TYPE_SYSTEM_RELATED_TYPES)
                .anyMatch(name -> StringUtils.equalsIgnoreCase(tableName, name + typeSystemSuffix));
    }

    private String getAdditionalSuffix(final String tableName, final DataBaseProvider dataBaseProvider) {
        if ((dataBaseProvider.isOracleUsed() || dataBaseProvider.isHanaUsed())
                && StringUtils.endsWith(tableName, LP_SUFFIX_UPPER)) {
            return LP_SUFFIX_UPPER;
        } else if (StringUtils.endsWithIgnoreCase(tableName, LP_SUFFIX)) {
            return LP_SUFFIX;
        } else {
            return StringUtils.EMPTY;
        }
    }

    private String getTableNameWithoutTypeSystemSuffix(final String tableName, final String suffix) {
        return StringUtils.removeEnd(tableName, suffix);
    }

    private String getTableNameWithoutAdditionalSuffix(final String tableName, final String suffix) {
        return StringUtils.removeEnd(tableName, suffix);
    }

    private String getTableNameWithoutPrefix(final String tableName, final String prefix) {
        return StringUtils.removeStartIgnoreCase(tableName, prefix);
    }

    private Set<CopyContext.DataCopyItem> createCopyItems(final MigrationContext context,
            final Set<TableCandidate> sourceTablesToMigrate, final Map<String, TableCandidate> targetTablesToMigrate)
            throws SQLException {
        final Set<CopyContext.DataCopyItem> copyItems = new HashSet<>();
        for (final TableCandidate sourceTableToMigrate : sourceTablesToMigrate) {
            final String targetTableKey = sourceTableToMigrate.getCommonTableName().toLowerCase();

            LOG.debug("Eligible Tables to Migrate = " + targetTableKey);
            if (targetTablesToMigrate.containsKey(targetTableKey)) {
                final TableCandidate targetTableToMigrate = targetTablesToMigrate.get(targetTableKey);
                copyItems.add(createCopyItem(context, sourceTableToMigrate, targetTableToMigrate));
            } else {
                throw new IllegalStateException("Target table must exists");
            }
        }
        return copyItems;
    }

    private CopyContext.DataCopyItem createCopyItem(final MigrationContext context, final TableCandidate sourceTable,
            final TableCandidate targetTable) throws SQLException {
        final String sourceTableName = sourceTable.getFullTableName();
        final String targetTableName = targetTable.getFullTableName();
        DataRepository sds = context.getDataSourceRepository();
        String sTableName = context.getItemTypeViewNameByTable(sourceTableName, sds);
        int batchSize = context.getReaderBatchSize(sourceTableName);
        final CopyContext.DataCopyItem dataCopyItem = new CopyContext.DataCopyItem(sTableName, targetTableName,
                batchSize);
        addColumnMappingsIfNecessary(context, sourceTable, dataCopyItem);
        return dataCopyItem;
    }

    private void addColumnMappingsIfNecessary(final MigrationContext context, final TableCandidate sourceTable,
            final CopyContext.DataCopyItem dataCopyItem) {
        if (sourceTable.getCommonTableName().equalsIgnoreCase(CommercedbsyncConstants.DEPLOYMENTS_TABLE)) {
            final String sourceTypeSystemName = context.getDataSourceRepository().getDataSourceConfiguration()
                    .getTypeSystemName();
            final String targetTypeSystemName = context.getDataTargetRepository().getDataSourceConfiguration()
                    .getTypeSystemName();
            // Add mapping to override the TypeSystemName value in target table
            if (!sourceTypeSystemName.equalsIgnoreCase(targetTypeSystemName)) {
                dataCopyItem.getColumnMap().put("TypeSystemName", targetTypeSystemName);
            }
        }
    }

    public void setDataCopyTableFilter(final DataCopyTableFilter dataCopyTableFilter) {
        this.dataCopyTableFilter = dataCopyTableFilter;
    }
}
