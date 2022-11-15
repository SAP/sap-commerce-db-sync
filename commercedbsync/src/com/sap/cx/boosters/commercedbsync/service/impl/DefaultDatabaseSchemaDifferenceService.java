/*
 *  Copyright: 2022 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.service.impl;

import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.sap.cx.boosters.commercedbsync.constants.CommercedbsyncConstants;
import de.hybris.platform.servicelayer.config.ConfigurationService;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.ddlutils.Platform;
import org.apache.ddlutils.model.Column;
import org.apache.ddlutils.model.Database;
import org.apache.ddlutils.model.Table;
import com.sap.cx.boosters.commercedbsync.TableCandidate;
import com.sap.cx.boosters.commercedbsync.context.MigrationContext;
import com.sap.cx.boosters.commercedbsync.filter.DataCopyTableFilter;
import com.sap.cx.boosters.commercedbsync.provider.CopyItemProvider;
import com.sap.cx.boosters.commercedbsync.repository.DataRepository;
import com.sap.cx.boosters.commercedbsync.service.DatabaseMigrationReportStorageService;
import com.sap.cx.boosters.commercedbsync.service.DatabaseSchemaDifferenceService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class DefaultDatabaseSchemaDifferenceService implements DatabaseSchemaDifferenceService {
	private static final Logger LOG = LoggerFactory.getLogger(DefaultDatabaseSchemaDifferenceService.class);

	private DataCopyTableFilter dataCopyTableFilter;
	private DatabaseMigrationReportStorageService databaseMigrationReportStorageService;
	private CopyItemProvider copyItemProvider;
	private ConfigurationService configurationService;

	@Override
	public String generateSchemaDifferencesSql(MigrationContext context) throws Exception {
		final int maxStageMigrations = context.getMaxTargetStagedMigrations();
		final Set<String> stagingPrefixes = findStagingPrefixes(context);
		String schemaSql = "";
		if (stagingPrefixes.size() > maxStageMigrations) {
			final Database databaseModelWithChanges = getDatabaseModelWithChanges4TableDrop(context);
			LOG.info("generateSchemaDifferencesSql..getDatabaseModelWithChanges4TableDrop.. - calibrating changes ");
			schemaSql = context.getDataTargetRepository().asPlatform().getDropTablesSql(databaseModelWithChanges, true);
			LOG.info("generateSchemaDifferencesSql - generated DDL SQLs for DROP. ");
		} else {
			LOG.info(
					"generateSchemaDifferencesSql..getDatabaseModelWithChanges4TableCreation - calibrating Schema changes ");
			final DatabaseStatus databaseModelWithChanges = getDatabaseModelWithChanges4TableCreation(context);
			if (databaseModelWithChanges.isHasSchemaDiff()) {
				LOG.info("generateSchemaDifferencesSql..Schema Diff found - now to generate the SQLs ");
				if (context.getDataTargetRepository().getDatabaseProvider().isHanaUsed()){
					schemaSql = context.getDataTargetRepository().asPlatform()
							.getAlterTablesSql(null ,context.getDataTargetRepository().getDataSourceConfiguration().getSchema(),null,databaseModelWithChanges.getDatabase());
				} else {
					schemaSql = context.getDataTargetRepository().asPlatform()
							.getAlterTablesSql(databaseModelWithChanges.getDatabase());
				}

				schemaSql = postProcess(schemaSql, context);
				LOG.info("generateSchemaDifferencesSql - generated DDL ALTER SQLs. ");
			}

		}

		return schemaSql;
	}

	/*
	 * ORACLE_TARGET - START This a TEMP fix, it is difficlt to get from from
	 * Sql Server NVARCHAR(255), NVARCHAR(MAX) to convert properly into to
	 * Orcale's VARCHAR2(255) and CLOB respectively. Therefore when the schema
	 * script output has VARCHAR2(2147483647) which is from SqlServer's
	 * NVARCHAR(max), then we just make it CLOB. Alternatively check if
	 * something can be done via the mappings in OracleDataRepository.
	 */
	private String postProcess(String schemaSql, final MigrationContext context) {
		if (context.getDataTargetRepository().getDatabaseProvider().isOracleUsed()) {
			schemaSql = schemaSql.replaceAll(CommercedbsyncConstants.MIGRATION_ORACLE_MAX,
					CommercedbsyncConstants.MIGRATION_ORACLE_VARCHAR24k);
			// another odd character that comes un in the SQL
			LOG.info("Changing the NVARCHAR2 " + schemaSql);
			schemaSql = schemaSql.replaceAll("NUMBER\\(10,0\\) DEFAULT \'\'\'\'\'\'", "NUMBER(10,0) DEFAULT 0");
		}
		return schemaSql;
	}
	// ORACLE_TARGET - END

	@Override
	public void executeSchemaDifferencesSql(final MigrationContext context, final String sql) throws Exception {

		if (!context.isSchemaMigrationEnabled()) {
			throw new RuntimeException("Schema migration is disabled. Check property:"
					+ CommercedbsyncConstants.MIGRATION_SCHEMA_ENABLED);
		}

		final Platform platform = context.getDataTargetRepository().asPlatform();
		final boolean continueOnError = false;
		final Connection connection = platform.borrowConnection();
		try {
			platform.evaluateBatch(connection, sql, continueOnError);
			LOG.info("Executed the following sql to change the schema:\n" + sql);
			writeReport(context, sql);
		} catch (final Exception e) {
			throw new RuntimeException("Could not execute Schema Diff Script", e);
		} finally {
			platform.returnConnection(connection);
		}
	}

	@Override
	public void executeSchemaDifferences(final MigrationContext context) throws Exception {
		executeSchemaDifferencesSql(context, generateSchemaDifferencesSql(context));
	}

	private Set<String> findDuplicateTables(final MigrationContext migrationContext) {
		try {
			final Set<String> stagingPrefixes = findStagingPrefixes(migrationContext);
			final Set<String> targetSet = migrationContext.getDataTargetRepository().getAllTableNames();
			return targetSet.stream()
					.filter(t -> stagingPrefixes.stream().anyMatch(p -> StringUtils.startsWithIgnoreCase(t, p)))
					.collect(Collectors.toSet());
		} catch (final Exception e) {
			LOG.error("Error occurred while trying to find duplicate tables", e);
		}
		return Collections.EMPTY_SET;
	}

	private Set<String> findStagingPrefixes(final MigrationContext context) throws Exception {
		final String currentSystemPrefix = configurationService.getConfiguration().getString("db.tableprefix");
		final String currentMigrationPrefix = context.getDataTargetRepository().getDataSourceConfiguration()
				.getTablePrefix();
		final Set<String> targetSet = context.getDataTargetRepository().getAllTableNames();
		final String deploymentsTable = CommercedbsyncConstants.DEPLOYMENTS_TABLE;
		final Set<String> detectedPrefixes = targetSet.stream().filter(t -> t.toLowerCase().endsWith(deploymentsTable))
				.filter(t -> !StringUtils.equalsIgnoreCase(t, currentSystemPrefix + deploymentsTable))
				.filter(t -> !StringUtils.equalsIgnoreCase(t, currentMigrationPrefix + deploymentsTable))
				.map(t -> StringUtils.removeEndIgnoreCase(t, deploymentsTable)).collect(Collectors.toSet());
		return detectedPrefixes;

	}

	private Database getDatabaseModelWithChanges4TableDrop(final MigrationContext context) {
		final Set<String> duplicateTables = findDuplicateTables(context);
		final Database database = context.getDataTargetRepository().asDatabase(true);
		// clear tables and add only the ones to be removed
		final Table[] tables = database.getTables();
		Stream.of(tables).forEach(t -> {
			database.removeTable(t);
		});
		duplicateTables.forEach(t -> {
			final Table table = ObjectUtils.defaultIfNull(database.findTable(t), new Table());
			table.setName(t);
			database.addTable(table);
		});
		return database;
	}

	protected DatabaseStatus getDatabaseModelWithChanges4TableCreation(final MigrationContext migrationContext)
			throws Exception {
		final DatabaseStatus dbStatus = new DatabaseStatus();

		final SchemaDifferenceResult differenceResult = getDifference(migrationContext);
		if (!differenceResult.hasDifferences()) {
			LOG.info("getDatabaseModelWithChanges4TableCreation - No Difference found in schema ");
			dbStatus.setDatabase(migrationContext.getDataTargetRepository().asDatabase());
			dbStatus.setHasSchemaDiff(false);
			return dbStatus;
		}
		final SchemaDifference targetDiff = differenceResult.getTargetSchema();
		final Database database = targetDiff.getDatabase();

		// add missing tables in target
		if (migrationContext.isAddMissingTablesToSchemaEnabled()) {
			final List<TableKeyPair> missingTables = targetDiff.getMissingTables();
			for (final TableKeyPair missingTable : missingTables) {
				final Table tableClone = (Table) differenceResult.getSourceSchema().getDatabase()
						.findTable(missingTable.getLeftName(), false).clone();
				tableClone.setName(missingTable.getRightName());
				tableClone.setCatalog(
						migrationContext.getDataTargetRepository().getDataSourceConfiguration().getCatalog());
				tableClone
						.setSchema(migrationContext.getDataTargetRepository().getDataSourceConfiguration().getSchema());
				database.addTable(tableClone);
				LOG.info("getDatabaseModelWithChanges4TableCreation - missingTable.getRightName() ="
						+ missingTable.getRightName() + ", missingTable.getLeftName() = " + missingTable.getLeftName());
			}
		}

		// add missing columns in target
		if (migrationContext.isAddMissingColumnsToSchemaEnabled()) {
			final ListMultimap<TableKeyPair, String> missingColumnsInTable = targetDiff.getMissingColumnsInTable();
			for (final TableKeyPair missingColumnsTable : missingColumnsInTable.keySet()) {
				final List<String> columns = missingColumnsInTable.get(missingColumnsTable);
				for (final String missingColumn : columns) {
					final Table missingColumnsTableModel = differenceResult.getSourceSchema().getDatabase()
							.findTable(missingColumnsTable.getLeftName(), false);
					final Column columnClone = (Column) missingColumnsTableModel.findColumn(missingColumn, false)
							.clone();
					LOG.info(" Column " + columnClone.getName() + ", Type = " + columnClone.getType() + ", Type Code "
							+ columnClone.getTypeCode() + ",size " + columnClone.getSize() + ", size as int "
							+ columnClone.getSizeAsInt());
					// columnClone.set
					final Table table = database.findTable(missingColumnsTable.getRightName(), false);
					Preconditions.checkState(table != null, "Data inconsistency: Table must exist.");
					table.addColumn(columnClone);
				}
			}
		}

		//remove superfluous tables in target
		if (migrationContext.isRemoveMissingTablesToSchemaEnabled()) {
			throw new UnsupportedOperationException("not yet implemented");
		}

		// remove superfluous columns in target
		if (migrationContext.isRemoveMissingColumnsToSchemaEnabled()) {
			final ListMultimap<TableKeyPair, String> superfluousColumnsInTable = differenceResult.getSourceSchema()
					.getMissingColumnsInTable();
			for (final TableKeyPair superfluousColumnsTable : superfluousColumnsInTable.keySet()) {
				final List<String> columns = superfluousColumnsInTable.get(superfluousColumnsTable);
				for (final String superfluousColumn : columns) {
					final Table table = database.findTable(superfluousColumnsTable.getLeftName(), false);
					Preconditions.checkState(table != null, "Data inconsistency: Table must exist.");
					final Column columnToBeRemoved = table.findColumn(superfluousColumn, false);
					// remove indices in case column is part of one
					Stream.of(table.getIndices()).filter(i -> i.hasColumn(columnToBeRemoved))
							.forEach(i -> table.removeIndex(i));
					table.removeColumn(columnToBeRemoved);
				}
			}
		}
		dbStatus.setDatabase(database);
		dbStatus.setHasSchemaDiff(true);
		LOG.info("getDatabaseModelWithChanges4TableCreation Schema Diff found -  done ");
		return dbStatus;
	}

	protected void writeReport(final MigrationContext migrationContext, final String differenceSql) {
		try {
			final String fileName = String.format("schemaChanges-%s.sql", LocalDateTime.now().getNano());
			databaseMigrationReportStorageService.store(fileName,
					new ByteArrayInputStream(differenceSql.getBytes(StandardCharsets.UTF_8)));
		} catch (final Exception e) {
			LOG.error("Error executing writing diff report", e);
		}
	}

	@Override
	public SchemaDifferenceResult getDifference(final MigrationContext migrationContext) throws Exception {
		try {
			LOG.info("reading source database model ...");
			migrationContext.getDataSourceRepository().asDatabase(true);
			LOG.info("reading target database model ...");
			migrationContext.getDataTargetRepository().asDatabase(true);

			LOG.info("computing SCHEMA diff, REF DB = "
					+ migrationContext.getDataTargetRepository().getDatabaseProvider().getDbName()
					+ "vs Checking in DB = "
					+ migrationContext.getDataSourceRepository().getDatabaseProvider().getDbName());
			final Set<TableCandidate> targetTableCandidates = copyItemProvider
					.getTargetTableCandidates(migrationContext);
			final SchemaDifference sourceSchemaDifference = computeDiff(migrationContext,
					migrationContext.getDataTargetRepository(), migrationContext.getDataSourceRepository(),
					targetTableCandidates);
			LOG.info("compute SCHMEA diff, REF DB ="
					+ migrationContext.getDataSourceRepository().getDatabaseProvider().getDbName()
					+ "vs Checking in DB = "
					+ migrationContext.getDataTargetRepository().getDatabaseProvider().getDbName());
			final Set<TableCandidate> sourceTableCandidates = copyItemProvider
					.getSourceTableCandidates(migrationContext);
			final SchemaDifference targetSchemaDifference = computeDiff(migrationContext,
					migrationContext.getDataSourceRepository(), migrationContext.getDataTargetRepository(),
					sourceTableCandidates);
			final SchemaDifferenceResult schemaDifferenceResult = new SchemaDifferenceResult(sourceSchemaDifference,
					targetSchemaDifference);
			LOG.info("Diff finished. Differences detected: " + schemaDifferenceResult.hasDifferences());

			return schemaDifferenceResult;
		} catch (final Exception e) {
			throw new RuntimeException("Error computing schema diff", e);
		}
	}

	protected String getSchemaDifferencesAsJson(final SchemaDifferenceResult schemaDifferenceResult) {
		final Gson gson = new GsonBuilder().setPrettyPrinting().create();
		return gson.toJson(schemaDifferenceResult);
	}

	private void logMigrationContext(final MigrationContext context) {
		if (context == null) {
			return;
		}
		LOG.info("--------MIGRATION CONTEXT- START----------");
		LOG.info("isAddMissingColumnsToSchemaEnabled=" + context.isAddMissingColumnsToSchemaEnabled());
		LOG.info("isAddMissingTablesToSchemaEnabled=" + context.isAddMissingTablesToSchemaEnabled());
		LOG.info("isAuditTableMigrationEnabled=" + context.isAuditTableMigrationEnabled());
		LOG.info("isBulkCopyEnabled=" + context.isBulkCopyEnabled());
		LOG.info("isClusterMode=" + context.isClusterMode());
		LOG.info("isDeletionEnabled=" + context.isDeletionEnabled());
		LOG.info("isDisableAllIndexesEnabled=" + context.isDisableAllIndexesEnabled());
		LOG.info("isDropAllIndexesEnabled=" + context.isDropAllIndexesEnabled());
		LOG.info("isFailOnErrorEnabled=" + context.isFailOnErrorEnabled());
		LOG.info("isIncrementalModeEnabled=" + context.isIncrementalModeEnabled());
		LOG.info("isMigrationTriggeredByUpdateProcess=" + context.isMigrationTriggeredByUpdateProcess());
		LOG.info("isRemoveMissingColumnsToSchemaEnabled=" + context.isRemoveMissingColumnsToSchemaEnabled());
		LOG.info("isRemoveMissingTablesToSchemaEnabled=" + context.isRemoveMissingTablesToSchemaEnabled());
		LOG.info("isSchemaMigrationAutoTriggerEnabled=" + context.isSchemaMigrationAutoTriggerEnabled());
		LOG.info("isSchemaMigrationEnabled=" + context.isSchemaMigrationEnabled());
		LOG.info("isTruncateEnabled=" + context.isTruncateEnabled());
		LOG.info("getIncludedTables=" + context.getIncludedTables());
		LOG.info("getExcludedTables=" + context.getExcludedTables());
		LOG.info("getIncrementalTables=" + context.getIncrementalTables());
		LOG.info("getTruncateExcludedTables=" + context.getTruncateExcludedTables());
		LOG.info("getCustomTables=" + context.getCustomTables());
		LOG.info("getIncrementalTimestamp=" + context.getIncrementalTimestamp());
		LOG.info(
				"Source TS Name=" + context.getDataSourceRepository().getDataSourceConfiguration().getTypeSystemName());
		LOG.info("Source TS Suffix ="
				+ context.getDataSourceRepository().getDataSourceConfiguration().getTypeSystemSuffix());
		LOG.info(
				"Target TS Name=" + context.getDataTargetRepository().getDataSourceConfiguration().getTypeSystemName());
		LOG.info("Target TS Suffix ="
				+ context.getDataTargetRepository().getDataSourceConfiguration().getTypeSystemSuffix());

		LOG.info("--------MIGRATION CONTEXT- END----------");
	}

	protected SchemaDifference computeDiff(final MigrationContext context, final DataRepository leftRepository,
			final DataRepository rightRepository, final Set<TableCandidate> leftCandidates) {
		logMigrationContext(context);
		final SchemaDifference schemaDifference = new SchemaDifference(rightRepository.asDatabase(),
				rightRepository.getDataSourceConfiguration().getTablePrefix());
		final Set<TableCandidate> leftDatabaseTables = getTables(context, leftRepository, leftCandidates);
		LOG.info("LEFT Repo = " + leftRepository.getDatabaseProvider().getDbName());
		LOG.info("RIGHT Repo = " + rightRepository.getDatabaseProvider().getDbName());

		try {
			LOG.debug(" All tables in LEFT Repo " + leftRepository.getAllTableNames());
			LOG.debug(" All tables in RIGHT Repo " + rightRepository.getAllTableNames());
		} catch (final Exception e) {
			LOG.error("Cannot fetch all Table Names" + e);
		}

		// LOG.info(" -------------------------------");
		for (final TableCandidate leftCandidate : leftDatabaseTables) {
			LOG.info(" Checking if Left Table exists --> " + leftCandidate.getFullTableName());
			final Table leftTable = leftRepository.asDatabase().findTable(leftCandidate.getFullTableName(), false);
			if (leftTable == null) {
				LOG.error(String.format("Table %s in DB %s cannot be found, but should exist",
						leftCandidate.getFullTableName(),
						leftRepository.getDataSourceConfiguration().getConnectionString()));
				continue;

				// throw new RuntimeException(String.format("Table %s in DB %s
				// cannot be found, but should exists",
				// leftCandidate.getFullTableName(),
				// leftRepository.getDataSourceConfiguration().getConnectionString()));
			}
			final String rightTableName = translateTableName(leftRepository, rightRepository, leftCandidate);
			final Table rightTable = rightRepository.asDatabase().findTable(rightTableName, false);
			if (rightTable == null) {
				schemaDifference.getMissingTables().add(new TableKeyPair(leftTable.getName(), rightTableName));
				LOG.info("MISSING Table !! --> " + leftTable.getName() + " searched for " + rightTableName);
			} else {
				// LOG.info(" FOUND Table --> " + rightTable.getName());
				final Column[] leftTableColumns = leftTable.getColumns();
				for (final Column leftTableColumn : leftTableColumns) {
					if (rightTable.findColumn(leftTableColumn.getName(), false) == null) {
						LOG.info("Missing column --> " + leftTableColumn.getName() + " -->" + leftTable.getName());
						schemaDifference.getMissingColumnsInTable().put(
								new TableKeyPair(leftTable.getName(), rightTable.getName()), leftTableColumn.getName());
					}
				}
			}
		}
		return schemaDifference;
	}

	private String translateTableName(final DataRepository leftRepository, final DataRepository rightRepository,
			final TableCandidate leftCandidate) {
		String translatedTableName = rightRepository.getDataSourceConfiguration().getTablePrefix()
				+ leftCandidate.getBaseTableName();
		if (leftCandidate.isTypeSystemRelatedTable()) {
			translatedTableName += rightRepository.getDataSourceConfiguration().getTypeSystemSuffix();
		}
		// ORCALE_TEMP - START
		/*
		 * if (!leftCandidate.getAdditionalSuffix().isEmpty() &&
		 * translatedTableName.toLowerCase().endsWith(leftCandidate.
		 * getAdditionalSuffix())) {
		 * //System.out.println("$$Translated name ends with LP " +
		 * translatedTableName); return translatedTableName; }
		 */
		// ORCALE_TEMP - END
		return translatedTableName + leftCandidate.getAdditionalSuffix();
	}

	private Set<TableCandidate> getTables(final MigrationContext context, final DataRepository repository,
			final Set<TableCandidate> candidates) {
		return candidates.stream().filter(c -> dataCopyTableFilter.filter(context).test(c.getCommonTableName()))
				.collect(Collectors.toSet());
	}

	public void setDataCopyTableFilter(final DataCopyTableFilter dataCopyTableFilter) {
		this.dataCopyTableFilter = dataCopyTableFilter;
	}

	public void setDatabaseMigrationReportStorageService(
			final DatabaseMigrationReportStorageService databaseMigrationReportStorageService) {
		this.databaseMigrationReportStorageService = databaseMigrationReportStorageService;
	}

	public void setConfigurationService(final ConfigurationService configurationService) {
		this.configurationService = configurationService;
	}

	public void setCopyItemProvider(final CopyItemProvider copyItemProvider) {
		this.copyItemProvider = copyItemProvider;
	}

	public static class SchemaDifferenceResult {
		private final SchemaDifference sourceSchema;
		private final SchemaDifference targetSchema;

		public SchemaDifferenceResult(final SchemaDifference sourceSchema, final SchemaDifference targetSchema) {
			this.sourceSchema = sourceSchema;
			this.targetSchema = targetSchema;
		}

		public SchemaDifference getSourceSchema() {
			return sourceSchema;
		}

		public SchemaDifference getTargetSchema() {
			return targetSchema;
		}

		public boolean hasDifferences() {
			final boolean hasMissingTargetTables = getTargetSchema().getMissingTables().size() > 0;
			final boolean hasMissingColumnsInTargetTable = getTargetSchema().getMissingColumnsInTable().size() > 0;
			final boolean hasMissingSourceTables = getSourceSchema().getMissingTables().size() > 0;
			final boolean hasMissingColumnsInSourceTable = getSourceSchema().getMissingColumnsInTable().size() > 0;
			return hasMissingTargetTables || hasMissingColumnsInTargetTable || hasMissingSourceTables
					|| hasMissingColumnsInSourceTable;
		}
	}

	class DatabaseStatus {
		private Database database;

		/**
		 * @return the database
		 */
		public Database getDatabase() {
			return database;
		}

		/**
		 * @param database
		 *            the database to set
		 */
		public void setDatabase(final Database database) {
			this.database = database;
		}

		/**
		 * @return the hasSchemaDiff
		 */
		public boolean isHasSchemaDiff() {
			return hasSchemaDiff;
		}

		/**
		 * @param hasSchemaDiff
		 *            the hasSchemaDiff to set
		 */
		public void setHasSchemaDiff(final boolean hasSchemaDiff) {
			this.hasSchemaDiff = hasSchemaDiff;
		}

		private boolean hasSchemaDiff;
	}

	public static class SchemaDifference {

		private final Database database;
		private final String prefix;

		private final List<TableKeyPair> missingTables = new ArrayList<>();
		private final ListMultimap<TableKeyPair, String> missingColumnsInTable = ArrayListMultimap.create();

		public SchemaDifference(final Database database, final String prefix) {
			this.database = database;
			this.prefix = prefix;

		}

		public Database getDatabase() {
			return database;
		}

		public String getPrefix() {
			return prefix;
		}

		public List<TableKeyPair> getMissingTables() {
			return missingTables;
		}

		public ListMultimap<TableKeyPair, String> getMissingColumnsInTable() {
			return missingColumnsInTable;
		}
	}

	public static class TableKeyPair {
		private final String leftName;
		private final String rightName;

		public TableKeyPair(final String leftName, final String rightName) {
			this.leftName = leftName;
			this.rightName = rightName;
		}

		public String getLeftName() {
			return leftName;
		}

		public String getRightName() {
			return rightName;
		}

		@Override
		public boolean equals(final Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			final TableKeyPair that = (TableKeyPair) o;
			return leftName.equals(that.leftName) && rightName.equals(that.rightName);
		}

		@Override
		public int hashCode() {
			return Objects.hash(leftName, rightName);
		}
	}

}