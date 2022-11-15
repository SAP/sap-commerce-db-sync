/*
 *  Copyright: 2022 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.context.impl;


import com.sap.cx.boosters.commercedbsync.constants.CommercedbsyncConstants;
import com.sap.cx.boosters.commercedbsync.profile.DataSourceConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.StringUtils;
import com.sap.cx.boosters.commercedbsync.context.MigrationContext;
import com.sap.cx.boosters.commercedbsync.repository.DataRepository;
import com.sap.cx.boosters.commercedbsync.repository.impl.DataRepositoryFactory;

import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

public class DefaultMigrationContext implements MigrationContext {
    private final DataRepository dataSourceRepository;
    private final DataRepository dataTargetRepository;
    protected boolean deletionEnabled;
    protected boolean lpTableMigrationEnabled;

    protected final Configuration configuration;

    public DefaultMigrationContext(final DataSourceConfiguration sourceDataSourceConfiguration,
                                   final DataSourceConfiguration targetDataSourceConfiguration,
                                   final DataRepositoryFactory dataRepositoryFactory,
                                   final Configuration configuration) throws Exception {
        this.dataSourceRepository = dataRepositoryFactory.create(sourceDataSourceConfiguration);
        this.dataTargetRepository = dataRepositoryFactory.create(targetDataSourceConfiguration);
        this.configuration = configuration;
        ensureDefaultLocale(configuration);
    }

    private void ensureDefaultLocale(Configuration configuration) {
        String localeProperty = configuration.getString(CommercedbsyncConstants.MIGRATION_LOCALE_DEFAULT);
        Locale locale = Locale.forLanguageTag(localeProperty);
        Locale.setDefault(locale);
    }


    @Override
    public DataRepository getDataSourceRepository() {
        return dataSourceRepository;
    }

    @Override
    public DataRepository getDataTargetRepository() {
        return dataTargetRepository;
    }

    @Override
    public boolean isMigrationTriggeredByUpdateProcess() {
        return getBooleanProperty(CommercedbsyncConstants.MIGRATION_TRIGGER_UPDATESYSTEM);
    }

    @Override
    public boolean isSchemaMigrationEnabled() {
        return getBooleanProperty(CommercedbsyncConstants.MIGRATION_SCHEMA_ENABLED);
    }

    @Override
    public boolean isAddMissingTablesToSchemaEnabled() {
        return getBooleanProperty(CommercedbsyncConstants.MIGRATION_SCHEMA_TARGET_TABLES_ADD_ENABLED);
    }

    @Override
    public boolean isRemoveMissingTablesToSchemaEnabled() {
        return getBooleanProperty(CommercedbsyncConstants.MIGRATION_SCHEMA_TARGET_TABLES_REMOVE_ENABLED);
    }

    @Override
    public boolean isAddMissingColumnsToSchemaEnabled() {
        return getBooleanProperty(CommercedbsyncConstants.MIGRATION_SCHEMA_TARGET_COLUMNS_ADD_ENABLED);
    }

    @Override
    public boolean isRemoveMissingColumnsToSchemaEnabled() {
        return getBooleanProperty(CommercedbsyncConstants.MIGRATION_SCHEMA_TARGET_COLUMNS_REMOVE_ENABLED);
    }

    @Override
    public boolean isSchemaMigrationAutoTriggerEnabled() {
        return getBooleanProperty(CommercedbsyncConstants.MIGRATION_SCHEMA_AUTOTRIGGER_ENABLED);
    }

    @Override
    public int getReaderBatchSize() {
        return getNumericProperty(CommercedbsyncConstants.MIGRATION_DATA_READER_BATCHSIZE);
    }

    @Override
    public boolean isTruncateEnabled() {
        return getBooleanProperty(CommercedbsyncConstants.MIGRATION_DATA_TRUNCATE_ENABLED);
    }

    @Override
    public boolean isAuditTableMigrationEnabled() {
        return getBooleanProperty(CommercedbsyncConstants.MIGRATION_DATA_TABLES_AUDIT_ENABLED);
    }

    @Override
    public Set<String> getTruncateExcludedTables() {
        return getListProperty(CommercedbsyncConstants.MIGRATION_DATA_TRUNCATE_EXCLUDED);
    }

    @Override
    public int getMaxParallelReaderWorkers() {
        return getNumericProperty(CommercedbsyncConstants.MIGRATION_DATA_WORKERS_READER_MAXTASKS);
    }

    @Override
    public int getMaxParallelWriterWorkers() {
        return getNumericProperty(CommercedbsyncConstants.MIGRATION_DATA_WORKERS_WRITER_MAXTASKS);
    }

    @Override
    public int getMaxWorkerRetryAttempts() {
        return getNumericProperty(CommercedbsyncConstants.MIGRATION_DATA_WORKERS_RETRYATTEMPTS);
    }


    @Override
    public int getMaxParallelTableCopy() {
        return getNumericProperty(CommercedbsyncConstants.MIGRATION_DATA_MAXPRALLELTABLECOPY);
    }

    @Override
    public boolean isFailOnErrorEnabled() {
        return getBooleanProperty(CommercedbsyncConstants.MIGRATION_DATA_FAILONEERROR_ENABLED);
    }

    @Override
    public Map<String, Set<String>> getExcludedColumns() {
        return getDynamicPropertyKeys(CommercedbsyncConstants.MIGRATION_DATA_COLUMNS_EXCLUDED);
    }

    public Map<String, Set<String>> getNullifyColumns() {
        return getDynamicPropertyKeys(CommercedbsyncConstants.MIGRATION_DATA_COLUMNS_NULLIFY);
    }

    @Override
    public Set<String> getCustomTables() {
        return getListProperty(CommercedbsyncConstants.MIGRATION_DATA_TABLES_CUSTOM);
    }

    @Override
    public Set<String> getExcludedTables() {
        return getListProperty(CommercedbsyncConstants.MIGRATION_DATA_TABLES_EXCLUDED);
    }

    @Override
    public Set<String> getIncludedTables() {
        return getListProperty(CommercedbsyncConstants.MIGRATION_DATA_TABLES_INCLUDED);
    }

    @Override
    public boolean isDropAllIndexesEnabled() {
        return getBooleanProperty(CommercedbsyncConstants.MIGRATION_DATA_INDICES_DROP_ENABLED);
    }

    @Override
    public boolean isDisableAllIndexesEnabled() {
        return getBooleanProperty(CommercedbsyncConstants.MIGRATION_DATA_INDICES_DISABLE_ENABLED);
    }

    @Override
    public Set<String> getDisableAllIndexesIncludedTables() {
        return getListProperty(CommercedbsyncConstants.MIGRATION_DATA_INDICES_DISABLE_INCLUDED);
    }


    @Override
    public boolean isClusterMode() {
        return getBooleanProperty(CommercedbsyncConstants.MIGRATION_CLUSTER_ENABLED);
    }

    @Override
    public boolean isIncrementalModeEnabled() {
        return getBooleanProperty(CommercedbsyncConstants.MIGRATION_DATA_INCREMENTAL_ENABLED);
    }

    @Override
    public Set<String> getIncrementalTables() {
        return getListProperty(CommercedbsyncConstants.MIGRATION_DATA_INCREMENTAL_TABLES);
    }

    @Override
    public Instant getIncrementalTimestamp() {
        String timeStamp = getStringProperty(CommercedbsyncConstants.MIGRATION_DATA_INCREMENTAL_TIMESTAMP);
        if (StringUtils.isEmpty(timeStamp)) {
            return null;
        }
        return ZonedDateTime.parse(timeStamp, DateTimeFormatter.ISO_ZONED_DATE_TIME).toInstant();
    }

    @Override
    public boolean isBulkCopyEnabled() {
        return getBooleanProperty(CommercedbsyncConstants.MIGRATION_DATA_BULKCOPY_ENABLED);
    }

    @Override
    public int getDataPipeTimeout() {
        return getNumericProperty(CommercedbsyncConstants.MIGRATION_DATA_PIPE_TIMEOUT);
    }

    @Override
    public int getDataPipeCapacity() {
        return getNumericProperty(CommercedbsyncConstants.MIGRATION_DATA_PIPE_CAPACITY);
    }

    @Override
    public String getMigrationReportConnectionString() {
        return getStringProperty(CommercedbsyncConstants.MIGRATION_DATA_REPORT_CONNECTIONSTRING);
    }

    @Override
    public int getMaxTargetStagedMigrations() {
        return getNumericProperty(CommercedbsyncConstants.MIGRATION_TARGET_MAX_STAGE_MIGRATIONS);
    }

    @Override
    public boolean isDeletionEnabled() {
        return this.deletionEnabled;
    }

    @Override
    public boolean isLpTableMigrationEnabled() {
        return this.lpTableMigrationEnabled;
    }

    @Override
    public void refreshSelf() {

    }

    @Override
    public int getStalledTimeout() {
        return getNumericProperty(CommercedbsyncConstants.MIGRATION_STALLED_TIMEOUT);
    }

    protected boolean getBooleanProperty(final String key) {
        return configuration.getBoolean(key);
    }

    protected int getNumericProperty(final String key) {
        return configuration.getInt(key);
    }

    protected String getStringProperty(final String key) {
        return configuration.getString(key);
    }

    private Set<String> getListProperty(final String key) {
        final String tables = configuration.getString(key);

        if (StringUtils.isEmpty(tables)) {
            return Collections.emptySet();
        }

        final Set<String> result = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        final String[] tablesArray = tables.split(",");
        result.addAll(Arrays.stream(tablesArray).collect(Collectors.toSet()));

        return result;
    }

    private Map<String, Set<String>> getDynamicPropertyKeys(final String key) {
        final Map<String, Set<String>> map = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        final Configuration subset = configuration.subset(key);
        final Iterator<String> keys = subset.getKeys();
        while (keys.hasNext()) {
            final String current = keys.next();
            map.put(current, getListProperty(key + "." + current));
        }
        return map;
    }

    private Map<String, String[]> getDynamicPropertyKeysValue(final String key) {
        final Map<String, String[]> map = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        final Configuration subset = configuration.subset(key);
        final Iterator<String> keys = subset.getKeys();

        while (keys.hasNext()) {
            final String current = keys.next();
            final String params = configuration.getString(key + "." + current);
            final String[] paramsArray = params.split(",");
            map.put(current, paramsArray);
        }
        return map;
    }
}
