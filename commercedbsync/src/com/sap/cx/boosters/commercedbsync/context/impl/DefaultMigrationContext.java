/*
 *  Copyright: 2023 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.context.impl;

import java.sql.SQLException;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

import com.sap.cx.boosters.commercedbsync.profile.DataSourceConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.StringUtils;

import com.google.common.base.Splitter;
import com.sap.cx.boosters.commercedbsync.constants.CommercedbsyncConstants;
import com.sap.cx.boosters.commercedbsync.context.MigrationContext;
import com.sap.cx.boosters.commercedbsync.profile.DataSourceConfigurationFactory;
import com.sap.cx.boosters.commercedbsync.repository.DataRepository;
import com.sap.cx.boosters.commercedbsync.repository.impl.DataRepositoryFactory;

public class DefaultMigrationContext implements MigrationContext {
    private final DataRepository dataSourceRepository;
    private final DataRepository dataTargetRepository;
    protected boolean deletionEnabled;
    protected boolean lpTableMigrationEnabled;

    protected final Configuration configuration;

    protected Set<String> tableViewNames;

    public DefaultMigrationContext(final DataRepositoryFactory dataRepositoryFactory,
            final DataSourceConfigurationFactory dataSourceConfigurationFactory, final Configuration configuration)
            throws Exception {
        this.configuration = configuration;
        ensureDefaultLocale(configuration);
        final Set<DataSourceConfiguration> inputDataSourceConfigurations = getInputProfiles().stream()
                .map(dataSourceConfigurationFactory::create).collect(Collectors.toSet());
        final Set<DataSourceConfiguration> outputDataSourceConfigurations = getOutputProfiles().stream()
                .map(dataSourceConfigurationFactory::create).collect(Collectors.toSet());
        this.dataSourceRepository = dataRepositoryFactory.create(this, inputDataSourceConfigurations);
        this.dataTargetRepository = dataRepositoryFactory.create(this, outputDataSourceConfigurations);
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
    public Integer getReaderBatchSize(final String tableName) {
        String tblConfKey = CommercedbsyncConstants.MIGRATION_DATA_READER_BATCHSIZE_FOR_TABLE.replace("{table}",
                tableName);
        return configuration.getInteger(tblConfKey, getReaderBatchSize());
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
    public int getDataPipeTimeout() {
        return getNumericProperty(CommercedbsyncConstants.MIGRATION_DATA_PIPE_TIMEOUT);
    }

    @Override
    public int getDataPipeCapacity() {
        return getNumericProperty(CommercedbsyncConstants.MIGRATION_DATA_PIPE_CAPACITY);
    }

    @Override
    public String getFileStorageConnectionString() {
        return getStringProperty(CommercedbsyncConstants.MIGRATION_FILE_STORAGE_CONNECTIONSTRING);
    }

    @Override
    public int getMaxTargetStagedMigrations() {
        return getNumericProperty(CommercedbsyncConstants.MIGRATION_TARGET_MAX_STAGE_MIGRATIONS);
    }

    @Override
    public boolean isDataExportEnabled() {
        return getBooleanProperty(CommercedbsyncConstants.MIGRATION_DATA_EXPORT_ENABLED);
    }

    @Override
    public boolean isSchedulerResumeEnabled() {
        return getBooleanProperty(CommercedbsyncConstants.MIGRATION_SCHEDULER_RESUME_ENABLED);
    }

    @Override
    public boolean isLogSql() {
        return getBooleanProperty(CommercedbsyncConstants.MIGRATION_LOG_SQL);
    }

    @Override
    public boolean isLogSqlParamsForSource() {
        return getBooleanProperty(CommercedbsyncConstants.MIGRATION_LOG_SQL_PARAMS_SOURCE);
    }

    @Override
    public int getSqlStoreMemoryFlushThreshold() {
        return getNumericProperty(CommercedbsyncConstants.MIGRATION_SQL_STORE_FLUSH_THRESHOLD);
    }

    @Override
    public String getFileStorageContainerName() {
        return getStringProperty(CommercedbsyncConstants.MIGRATION_FILE_STORAGE_CONTAINER_NAME);
    }

    @Override
    public Set<String> getInputProfiles() {
        return getListProperty(CommercedbsyncConstants.MIGRATION_INPUT_PROFILES);
    }

    @Override
    public Set<String> getOutputProfiles() {
        return getListProperty(CommercedbsyncConstants.MIGRATION_OUTPUT_PROFILES);
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
    public boolean isFullDatabaseMigration() {
        return getBooleanProperty(CommercedbsyncConstants.MIGRATION_DATA_FULLDATABASE);
    }

    @Override
    public void setFullDatabaseMigrationEnabled(final boolean enabled) {
        this.configuration.setProperty(CommercedbsyncConstants.MIGRATION_DATA_FULLDATABASE, Boolean.toString(enabled));
    }

    @Override
    public void refreshSelf() {
        // resetting "cached" view names as they may have changed when reusing a context
        // in job iterations.
        this.tableViewNames = null;
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

    protected Set<String> getListProperty(final String key) {
        final String tables = configuration.getString(key);

        if (StringUtils.isEmpty(tables)) {
            return Collections.emptySet();
        }

        final Set<String> result = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        result.addAll(Splitter.on(",").omitEmptyStrings().trimResults().splitToList(tables));

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

    private Map<String, String> getDynamicRawProperties(final String key) {
        final Map<String, String> map = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        final Configuration subset = configuration.subset(key);
        final Iterator<String> keys = subset.getKeys();
        while (keys.hasNext()) {
            final String current = keys.next();
            Optional.ofNullable(subset.getString(current)).ifPresent(value -> map.put(current, value));
        }
        return map;
    }

    @Override
    public String getItemTypeViewNamePattern() {
        return getStringProperty(CommercedbsyncConstants.MIGRATION_DB_VIEW_NAME_PATTERN);
    }

    @Override
    public String getItemTypeViewNameByTable(String tableName, DataRepository repository) throws SQLException {
        if (tableViewNames == null) {
            tableViewNames = repository.getAllViewNames();
        }
        String possibleVieName = String.format(StringUtils.trimToEmpty(getItemTypeViewNamePattern()), tableName);
        return tableViewNames.contains(possibleVieName) ? possibleVieName : tableName;
    }

    @Override
    public String getViewWhereClause(final String tableName) {
        String whereConfigKey = CommercedbsyncConstants.MIGRATION_DATA_VIEW_TBL_JOIN_WHERE.replace("{table}",
                tableName);
        String fromSection = configuration.getString(whereConfigKey);
        if (StringUtils.isBlank(fromSection.trim())) {
            fromSection = tableName;
        }
        return fromSection;
    }

    @Override
    public Map<String, String> getCustomColumnsForView(final String tableName) {
        String tblConfigKey = CommercedbsyncConstants.MIGRATION_DATA_VIEW_COL_REPLACEMENT.replace("{table}", tableName);
        String trimToTable = tblConfigKey.replace(".{column}", "");
        return getDynamicRawProperties(trimToTable);
    }

    @Override
    public Set<String> getTablesForViews() {
        Set<String> tables = new HashSet<>();
        String str = CommercedbsyncConstants.MIGRATION_DATA_VIEW_TBL_GENERATION;
        // prefix before table placeholder
        String key = str.substring(0, str.indexOf("{") - 1);
        final Configuration subset = configuration.subset(key);
        final Iterator<String> keys = subset.getKeys();
        while (keys.hasNext()) {
            final String current = keys.next();
            // trim from common prefix
            String subkey = current.replace(key, "");
            List<String> subkeyList = Splitter.on(".").splitToList(subkey);
            if (subkeyList.size() == 2 && "enabled".equals(subkeyList.get(1))) {
                boolean val = subset.getBoolean(current, false);
                if (val) {
                    String tablename = Splitter.on(".").splitToList(subkey).get(0);
                    tables.add(tablename);
                }
            }
        }
        return tables;
    }

    @Override
    public String getViewColumnPrefixFor(final String tableName) {
        return configuration.getString("migration.data.view.t." + tableName + ".columnPrefix");
    }
}
