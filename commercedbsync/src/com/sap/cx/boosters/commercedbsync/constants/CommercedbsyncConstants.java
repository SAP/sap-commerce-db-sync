/*
 *  Copyright: 2023 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.constants;

/**
 * Global class for all Commercedbsync constants. You can add global constants
 * for your extension into this class.
 */
public final class CommercedbsyncConstants extends GeneratedCommercedbsyncConstants {
    public static final String EXTENSIONNAME = "commercedbsync";
    public static final String PROPERTIES_PREFIX = "migration";
    public static final String MIGRATION_TRIGGER_UPDATESYSTEM = "migration.trigger.updatesystem";
    public static final String MIGRATION_SCHEMA_ENABLED = "migration.schema.enabled";
    public static final String MIGRATION_ANONYMIZER_ENABLED = "migration.anonymizer.enabled";
    public static final String MIGRATION_SCHEMA_TARGET_TABLES_ADD_ENABLED = "migration.schema.target.tables.add.enabled";
    public static final String MIGRATION_SCHEMA_TARGET_TABLES_REMOVE_ENABLED = "migration.schema.target.tables.remove.enabled";
    public static final String MIGRATION_SCHEMA_TARGET_COLUMNS_ADD_ENABLED = "migration.schema.target.columns.add.enabled";
    public static final String MIGRATION_SCHEMA_TARGET_COLUMNS_REMOVE_ENABLED = "migration.schema.target.columns.remove.enabled";
    public static final String MIGRATION_TARGET_MAX_STAGE_MIGRATIONS = "migration.ds.target.db.max.stage.migrations";
    public static final String MIGRATION_DATA_EXPORT_ENABLED = "migration.data.export.enabled";
    public static final String MIGRATION_SCHEMA_AUTOTRIGGER_ENABLED = "migration.schema.autotrigger.enabled";
    public static final String MIGRATION_DATA_FULLDATABASE = "migration.data.fulldatabase.enabled";
    public static final String MIGRATION_DATA_READER_BATCHSIZE = "migration.data.reader.batchsize";
    public static final String MIGRATION_DATA_READER_BATCHSIZE_FOR_TABLE = "migration.data.reader.batchsize.{table}";
    public static final String MIGRATION_CLUSTER_CHUNK_SIZE = "migration.cluster.chunk.size";
    public static final String MIGRATION_CLUSTER_CHUNK_SIZE_FOR_TABLE = "migration.cluster.chunk.size.{table}";
    public static final String MIGRATION_DATA_TRUNCATE_ENABLED = "migration.data.truncate.enabled";
    public static final String MIGRATION_DATA_TRUNCATE_EXCLUDED = "migration.data.truncate.excluded";
    public static final String MIGRATION_DATA_WORKERS_READER_MAXTASKS = "migration.data.workers.reader.maxtasks";
    public static final String MIGRATION_DATA_WORKERS_WRITER_MAXTASKS = "migration.data.workers.writer.maxtasks";
    public static final String MIGRATION_DATA_WORKERS_RETRYATTEMPTS = "migration.data.workers.retryattempts";
    public static final String MIGRATION_DATA_MAXPRALLELTABLECOPY = "migration.data.maxparalleltablecopy";
    public static final String MIGRATION_DATA_FAILONEERROR_ENABLED = "migration.data.failonerror.enabled";
    public static final String MIGRATION_DATA_COLUMNS_EXCLUDED = "migration.data.columns.excluded";
    public static final String MIGRATION_DATA_COLUMNS_NULLIFY = "migration.data.columns.nullify";
    public static final String MIGRATION_DATA_INDICES_DROP_ENABLED = "migration.data.indices.drop.enabled";
    public static final String MIGRATION_DATA_INDICES_DISABLE_ENABLED = "migration.data.indices.disable.enabled";
    public static final String MIGRATION_DATA_INDICES_DISABLE_INCLUDED = "migration.data.indices.disable.included";
    public static final String MIGRATION_DATA_TABLES_AUDIT_ENABLED = "migration.data.tables.audit.enabled";
    public static final String MIGRATION_DATA_TABLES_CUSTOM = "migration.data.tables.custom";
    public static final String MIGRATION_DATA_TABLES_EXCLUDED = "migration.data.tables.excluded";
    public static final String MIGRATION_DATA_TABLES_INCLUDED = "migration.data.tables.included";
    public static final String MIGRATION_CLUSTER_ENABLED = "migration.cluster.enabled";
    public static final String MIGRATION_DATA_INCREMENTAL_ENABLED = "migration.data.incremental.enabled";
    public static final String MIGRATION_DATA_INCREMENTAL_TABLES = "migration.data.incremental.tables";
    public static final String MIGRATION_DATA_INCREMENTAL_TIMESTAMP = "migration.data.incremental.timestamp";
    public static final String MIGRATION_DATA_PIPE_TIMEOUT = "migration.data.pipe.timeout";
    public static final String MIGRATION_DATA_PIPE_CAPACITY = "migration.data.pipe.capacity";
    public static final String MIGRATION_STALLED_TIMEOUT = "migration.stalled.timeout";
    public static final String MIGRATION_FILE_STORAGE_CONNECTIONSTRING = "migration.data.report.connectionstring";
    public static final String MIGRATION_DATATYPE_CHECK = "migration.datatype.check";
    public static final String MIGRATION_TABLESPREFIX = "MIGRATIONTOOLKIT_";
    public static final String MIGRATION_SCHEDULER_RESUME_ENABLED = "migration.scheduler.resume.enabled";
    public static final String MIGRATION_LOG_SQL = "migration.log.sql";
    public static final String MIGRATION_LOG_SQL_PARAMS_SOURCE = "migration.log.sql.source.showparameters";
    public static final String MIGRATION_SQL_STORE_FLUSH_THRESHOLD = "migration.log.sql.memory.flush.threshold.nbentries";
    public static final String MIGRATION_FILE_STORAGE_CONTAINER_NAME = "migration.data.filestorage.container.name";
    public static final String MIGRATION_INPUT_PROFILES = "migration.input.profiles";
    public static final String MIGRATION_OUTPUT_PROFILES = "migration.output.profiles";
    public static final String MIGRATION_PROFILING = "migration.profiling";
    public static final String MIGRATION_PROFILING_MEMORY_MIN = "migration.memory.min";
    public static final String MIGRATION_PROFILING_MEMORY_ATTEMPTS = "migration.memory.attempts";
    public static final String MIGRATION_PROFILING_MEMORY_WAIT = "migration.memory.wait";

    public static final String MIGRATION_DATA_READTASK_KEEPALIVE_SECONDS = "migration.data.readtask.keepaliveseconds";
    public static final String MIGRATION_DATA_READTASK_QUEUE_CAPACITY = "migration.data.readtask.queuecapacity";
    public static final String MIGRATION_DATA_READTASK_ALLOWTIMEOUT = "migration.data.readtask.allowtimeout";
    public static final String MIGRATION_DATA_WRITETASK_KEEPALIVE_SECONDS = "migration.data.writetask.keepaliveseconds";
    public static final String MIGRATION_DATA_WRITETASK_ALLOWTIMEOUT = "migration.data.writetask.allowtimeout";

    public static final String MIGRATION_DATA_WORKERS_READER_KEEPALIVESECONDS = "migration.data.workers.reader.keepaliveseconds";
    public static final String MIGRATION_DATA_WORKERS_WRITER_KEEPALIVESECONDS = "migration.data.workers.writer.keepaliveseconds";

    public static final String MDC_MIGRATIONID = "migrationID";
    public static final String MDC_SCHEMADIFFID = "schemaDiffID";
    public static final String MDC_PIPELINE = "pipeline";
    public static final String MDC_CLUSTERID = "clusterID";

    public static final String DEPLOYMENTS_TABLE = "ydeployments";

    // Masking
    public static final String MIGRATION_REPORT_MASKED_PROPERTIES = "migration.properties.masked";
    public static final String MASKED_VALUE = "***";

    // Locale
    public static final String MIGRATION_LOCALE_DEFAULT = "migration.locale.default";

    // Incremental support
    public static final String MIGRATION_DATA_INCREMENTAL_DELETIONS_ITEMTYPES = "migration.data.incremental.deletions.itemtypes";
    public static final String MIGRATION_DATA_INCREMENTAL_DELETIONS_TYPECODES = "migration.data.incremental.deletions.typecodes";
    public static final String MIGRATION_DATA_INCREMENTAL_DELETIONS_ITEMTYPES_ENABLED = "migration.data.incremental.deletions.itemtypes.enabled";
    public static final String MIGRATION_DATA_INCREMENTAL_DELETIONS_TYPECODES_ENABLED = "migration.data.incremental.deletions.typecodes.enabled";

    public static final String MIGRATION_ORACLE_MAX = "VARCHAR2\\(2147483647\\)";
    public static final String MIGRATION_ORACLE_CLOB = "CLOB";
    public static final String MIGRATION_ORACLE_VARCHAR24k = "VARCHAR2(4000)";

    // DB View support
    public static final String MIGRATION_DB_VIEW_NAME_PATTERN = "migration.data.view.name.pattern";

    // DDL View Generation
    // property

    public static final String MIGRATION_DATA_VIEW_TBL_GENERATION = "migration.data.view.t.{table}.enabled";
    public static final String MIGRATION_DATA_VIEW_TBL_JOIN_WHERE = "migration.data.view.t.{table}.joinWhereClause";
    public static final String MIGRATION_DATA_VIEW_TBL_COL_PREFIX = "migration.data.view.t.{table}.columnPrefix";
    public static final String MIGRATION_DATA_VIEW_COL_REPLACEMENT = "migration.data.view.t.{table}.columnTransformation.{column}";

    // MSSQL Post Processing
    public static final String MIGRATION_DATA_MSSQL_UPDATE_STATISTICS_ENABLED = "migration.data.mssql.update.statistics.enabled";

    private CommercedbsyncConstants() {
        // empty to avoid instantiating this constant class
    }

}
