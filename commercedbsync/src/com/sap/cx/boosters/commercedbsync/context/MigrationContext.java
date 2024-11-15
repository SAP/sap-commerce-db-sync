/*
 *  Copyright: 2023 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.context;

import com.sap.cx.boosters.commercedbsync.repository.DataRepository;

import java.sql.SQLException;
import java.time.Instant;
import java.util.Map;
import java.util.Set;

/**
 * The MigrationContext contains all information needed to perform a Source ->
 * Target Migration
 */
public interface MigrationContext {
    DataRepository getDataSourceRepository();

    DataRepository getDataTargetRepository();

    boolean isMigrationTriggeredByUpdateProcess();

    boolean isSchemaMigrationEnabled();

    boolean isAnonymizerEnabled();

    boolean isAddMissingTablesToSchemaEnabled();

    boolean isRemoveMissingTablesToSchemaEnabled();

    boolean isAddMissingColumnsToSchemaEnabled();

    boolean isRemoveMissingColumnsToSchemaEnabled();

    boolean isSchemaMigrationAutoTriggerEnabled();

    int getReaderBatchSize();

    /**
     * Retrieves the batch size for an individual table if available.
     *
     * @param tableName
     * @return int if configured or null if not defined for the given tableName
     */
    Integer getReaderBatchSize(final String tableName);

    long getClusterChunkSize();

    /**
     * Retrieves the chunk size for an individual table if available.
     *
     * @param tableName
     * @return int if configured or null if not defined for the given tableName
     */
    Long getClusterChunkSize(final String tableName);

    boolean isTruncateEnabled();

    boolean isAuditTableMigrationEnabled();

    Set<String> getTruncateExcludedTables();

    int getMaxParallelReaderWorkers();

    int getMaxParallelWriterWorkers();

    int getMaxParallelTableCopy();

    int getMaxWorkerRetryAttempts();

    boolean isFailOnErrorEnabled();

    Map<String, Set<String>> getExcludedColumns();

    Map<String, Set<String>> getNullifyColumns();

    Set<String> getCustomTables();

    Set<String> getExcludedTables();

    Set<String> getIncludedTables();

    boolean isDropAllIndexesEnabled();

    boolean isDisableAllIndexesEnabled();

    Set<String> getDisableAllIndexesIncludedTables();

    boolean isClusterMode();

    boolean isIncrementalModeEnabled();

    Set<String> getIncrementalTables();

    Instant getIncrementalTimestamp();

    int getDataPipeTimeout();

    int getDataPipeCapacity();

    int getStalledTimeout();

    String getFileStorageConnectionString();

    int getMaxTargetStagedMigrations();

    boolean isDataExportEnabled();

    boolean isDeletionEnabled();

    boolean isLpTableMigrationEnabled();

    boolean isSchedulerResumeEnabled();

    boolean isMssqlUpdateStatisticsEnabled();

    boolean isFullDatabaseMigration();

    void setFullDatabaseMigrationEnabled(boolean enabled);

    boolean isProfiling();

    long getMemoryMin();

    int getMemoryMaxAttempts();

    int getMemoryWait();

    void refreshSelf();

    /**
     * String value which defines name of the view which should be looked up in
     * database and check if matches by name. String pattern should be compatible
     * with {@code String.format()} pattern. E.g.<br>
     *
     * <pre>
     * String pattern    : "%s_view"
     * item type table   :  "products"
     * Searched view name: "products_view"
     * </pre>
     *
     * @return by default {@code null}. If setting is set in properties, it will
     *         return value defined.
     */
    String getItemTypeViewNamePattern();

    boolean isLogSql();

    boolean isLogSqlParamsForSource();

    int getSqlStoreMemoryFlushThreshold();

    String getFileStorageContainerName();

    Set<String> getInputProfiles();

    Set<String> getOutputProfiles();

    /**
     * Returns string value which is table/view name for particular table name,
     * which follows ItemType View pattern name.
     *
     * @return by default returns view name, as fallback, it will return origin
     *         table name
     * @throws SQLException
     *             when DB error occurs
     */
    String getItemTypeViewNameByTable(String tableName, DataRepository repository) throws SQLException;

    /**
     * Returns string value which is custom view name for particular table name,
     * which follows ItemType View pattern name.
     *
     * @return by default returns view name, as fallback, it will return origin
     *         table name
     * @throws SQLException
     *             when DB error occurs
     */
    String getViewWhereClause(final String tableName);

    /**
     * Returns list of replacements for particular column from table
     * <code>tableName</code>. That returns only custom ones. If configuration say:
     *
     * <code>
     *
     * </code>
     *
     * @param tableName
     *            table which should be filtered by from properties
     * @return map for original column to what it should return
     */
    Map<String, String> getCustomColumnsForView(final String tableName);

    /**
     * Return list of table names which have enabled view generation
     *
     * @return
     */
    Set<String> getTablesForViews();

    String getViewColumnPrefixFor(String tableName);
}
