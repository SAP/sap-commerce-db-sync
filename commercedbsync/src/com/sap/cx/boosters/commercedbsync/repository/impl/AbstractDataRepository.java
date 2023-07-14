/*
 *  Copyright: 2023 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.repository.impl;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import javax.sql.DataSource;

import com.sap.cx.boosters.commercedbsync.context.MigrationContext;
import com.sap.cx.boosters.commercedbsync.logging.JDBCQueriesStore;
import com.sap.cx.boosters.commercedbsync.logging.LoggingConnectionWrapper;
import de.hybris.bootstrap.ddl.PropertiesLoader;
import de.hybris.platform.core.Registry;
import de.hybris.platform.core.TenantPropertiesLoader;
import org.apache.commons.lang3.StringUtils;
import org.apache.ddlutils.Platform;
import org.apache.ddlutils.model.Database;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.Resource;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;

import com.google.common.base.Joiner;
import com.sap.cx.boosters.commercedbsync.MarkersQueryDefinition;
import com.sap.cx.boosters.commercedbsync.OffsetQueryDefinition;
import com.sap.cx.boosters.commercedbsync.SeekQueryDefinition;
import com.sap.cx.boosters.commercedbsync.TypeSystemTable;
import com.sap.cx.boosters.commercedbsync.constants.CommercedbsyncConstants;
import com.sap.cx.boosters.commercedbsync.dataset.DataColumn;
import com.sap.cx.boosters.commercedbsync.dataset.DataSet;
import com.sap.cx.boosters.commercedbsync.dataset.impl.DefaultDataColumn;
import com.sap.cx.boosters.commercedbsync.dataset.impl.DefaultDataSet;
import com.sap.cx.boosters.commercedbsync.datasource.MigrationDataSourceFactory;
import com.sap.cx.boosters.commercedbsync.datasource.impl.DefaultMigrationDataSourceFactory;
import com.sap.cx.boosters.commercedbsync.profile.DataSourceConfiguration;
import com.sap.cx.boosters.commercedbsync.repository.DataRepository;
import com.sap.cx.boosters.commercedbsync.service.DatabaseMigrationDataTypeMapperService;

import de.hybris.bootstrap.ddl.DatabaseSettings;
import de.hybris.bootstrap.ddl.HybrisPlatformFactory;
import de.hybris.bootstrap.ddl.tools.persistenceinfo.PersistenceInformation;

/**
 * Implementation of basic operations for accessing repositories
 */
public abstract class AbstractDataRepository implements DataRepository {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractDataRepository.class);

    public static final String LP_SUFFIX = "lp";

    // one store per data repository
    private final JDBCQueriesStore jdbcQueriesStore;
    private final Map<String, DataSource> dataSourceHolder = new ConcurrentHashMap<>();

    private final DataSourceConfiguration dataSourceConfiguration;
    private final MigrationDataSourceFactory migrationDataSourceFactory;
    private final DatabaseMigrationDataTypeMapperService databaseMigrationDataTypeMapperService;
    private final MigrationContext migrationContext;
    private Platform platform;
    private Database database;

    protected AbstractDataRepository(MigrationContext migrationContext, DataSourceConfiguration dataSourceConfiguration,
            DatabaseMigrationDataTypeMapperService databaseMigrationDataTypeMapperService) {
        this(migrationContext, dataSourceConfiguration, new DefaultMigrationDataSourceFactory(),
                databaseMigrationDataTypeMapperService);
    }

    protected AbstractDataRepository(MigrationContext migrationContext, DataSourceConfiguration dataSourceConfiguration,
            MigrationDataSourceFactory migrationDataSourceFactory,
            DatabaseMigrationDataTypeMapperService databaseMigrationDataTypeMapperService) {
        this.migrationContext = migrationContext;
        this.dataSourceConfiguration = dataSourceConfiguration;
        this.migrationDataSourceFactory = migrationDataSourceFactory;
        this.databaseMigrationDataTypeMapperService = databaseMigrationDataTypeMapperService;
        this.jdbcQueriesStore = new JDBCQueriesStore(getDataSourceConfiguration().getConnectionString(),
                migrationContext, migrationContext.getInputProfiles().contains(dataSourceConfiguration.getProfile()));
    }

    @Override
    public DataSourceConfiguration getDataSourceConfiguration() {
        return dataSourceConfiguration;
    }

    @Override
    public DataSource getDataSource() {
        return dataSourceHolder.computeIfAbsent("DATASOURCE",
                s -> migrationDataSourceFactory.create(dataSourceConfiguration));
    }

    @Override
    public DataSource getDataSourcePrimary() {
        return dataSourceHolder.computeIfAbsent("DATASOURCEPRIMARY", s -> {
            DataSource primaryDataSource = getDataSource();
            final String connectionStringPrimary = dataSourceConfiguration.getConnectionStringPrimary();
            if (StringUtils.isNotBlank(connectionStringPrimary)) {
                final Map<String, Object> dataSourceConfigurationMap = new HashMap<>();
                dataSourceConfigurationMap.put("connection.url", connectionStringPrimary);
                dataSourceConfigurationMap.put("driver", dataSourceConfiguration.getDriver());
                dataSourceConfigurationMap.put("username", dataSourceConfiguration.getUserName());
                dataSourceConfigurationMap.put("password", dataSourceConfiguration.getPassword());
                dataSourceConfigurationMap.put("pool.size.max", Integer.valueOf(1));
                dataSourceConfigurationMap.put("pool.size.idle.min", Integer.valueOf(1));
                dataSourceConfigurationMap.put("registerMbeans", Boolean.FALSE);

                primaryDataSource = migrationDataSourceFactory.create(dataSourceConfigurationMap);
            }

            return primaryDataSource;
        });
    }

    public Connection getConnection() throws SQLException {
        // Only wrap with the logging behavior if logSql is true
        if (migrationContext.isLogSql()) {
            boolean logParameters = jdbcQueriesStore.isSourceDB() && migrationContext.isLogSqlParamsForSource();
            return new LoggingConnectionWrapper(getDataSource().getConnection(), jdbcQueriesStore, logParameters);
        } else {
            return getDataSource().getConnection();
        }
    }

    @Override
    public int executeUpdateAndCommit(String updateStatement) throws SQLException {
        try (Connection conn = getConnectionForUpdateAndCommit(); Statement statement = conn.createStatement()) {
            return statement.executeUpdate(updateStatement);
        }
    }

    public Connection getConnectionForUpdateAndCommit() throws SQLException {
        Connection connection = getDataSource().getConnection();
        connection.setAutoCommit(true);
        return connection;
    }

    @Override
    public int executeUpdateAndCommitOnPrimary(final String updateStatement) throws SQLException {
        try (final Connection conn = getConnectionFromPrimary(); final Statement statement = conn.createStatement()) {
            return statement.executeUpdate(updateStatement);
        }
    }

    public Connection getConnectionFromPrimary() throws SQLException {
        final Connection connection = getDataSourcePrimary().getConnection();
        connection.setAutoCommit(true);
        return connection;
    }

    @Override
    public void runSqlScript(Resource resource) {
        final ResourceDatabasePopulator databasePopulator = new ResourceDatabasePopulator(resource);
        databasePopulator.setIgnoreFailedDrops(true);
        databasePopulator.execute(getDataSource());
    }

    @Override
    public void runSqlScriptOnPrimary(Resource resource) {
        final ResourceDatabasePopulator databasePopulator = new ResourceDatabasePopulator(resource);
        databasePopulator.setIgnoreFailedDrops(true);
        databasePopulator.execute(getDataSourcePrimary());
    }

    @Override
    public float getDatabaseUtilization() throws SQLException {
        throw new UnsupportedOperationException("Must be added in the specific repository implementation");
    }

    @Override
    public int truncateTable(String table) throws SQLException {
        return executeUpdateAndCommit(String.format("truncate table %s", table));
    }

    @Override
    public long getRowCount(String table) throws SQLException {
        List<String> conditionsList = new ArrayList<>(1);
        processDefaultConditions(table, conditionsList);
        String[] conditions = null;
        if (!conditionsList.isEmpty()) {
            conditions = conditionsList.toArray(new String[conditionsList.size()]);
        }
        try (Connection connection = getConnection();
                Statement stmt = connection.createStatement();
                ResultSet resultSet = stmt.executeQuery(
                        String.format("select count(*) from %s where %s", table, expandConditions(conditions)))) {
            long value = 0;
            if (resultSet.next()) {
                value = resultSet.getLong(1);
            }
            return value;
        }
    }

    @Override
    public long getRowCountModifiedAfter(String table, Instant time, boolean isDeletionEnabled,
            boolean lpTableMigrationEnabled) throws SQLException {
        return getRowCountModifiedAfter(table, time);
    }

    @Override
    public long getRowCountModifiedAfter(String table, Instant time) throws SQLException {
        List<String> conditionsList = new ArrayList<>(1);
        processDefaultConditions(table, conditionsList);
        String[] conditions = null;
        if (!conditionsList.isEmpty()) {
            conditions = conditionsList.toArray(new String[conditionsList.size()]);
        }
        try (Connection connection = getConnection()) {
            try (PreparedStatement stmt = connection.prepareStatement(String.format(
                    "select count(*) from %s where modifiedts > ? AND %s", table, expandConditions(conditions)))) {
                stmt.setTimestamp(1, Timestamp.from(time));
                ResultSet resultSet = stmt.executeQuery();
                long value = 0;
                if (resultSet.next()) {
                    value = resultSet.getLong(1);
                }
                return value;
            }
        }
    }

    @Override
    public DataSet getAll(String table) throws Exception {
        List<String> conditionsList = new ArrayList<>(1);
        processDefaultConditions(table, conditionsList);
        String[] conditions = null;
        if (!conditionsList.isEmpty()) {
            conditions = conditionsList.toArray(new String[conditionsList.size()]);
        }
        try (Connection connection = getConnection();
                Statement stmt = connection.createStatement();
                ResultSet resultSet = stmt.executeQuery(
                        String.format("select * from %s where %s", table, expandConditions(conditions)))) {
            return convertToDataSet(resultSet);
        }
    }

    @Override
    public DataSet getAllModifiedAfter(String table, Instant time) throws Exception {
        List<String> conditionsList = new ArrayList<>(1);
        processDefaultConditions(table, conditionsList);
        String[] conditions = null;
        if (!conditionsList.isEmpty()) {
            conditions = conditionsList.toArray(new String[conditionsList.size()]);
        }
        try (Connection connection = getConnection()) {
            try (PreparedStatement stmt = connection.prepareStatement(String
                    .format("select * from %s where modifiedts > ? and %s", table, expandConditions(conditions)))) {
                stmt.setTimestamp(1, Timestamp.from(time));
                ResultSet resultSet = stmt.executeQuery();
                return convertToDataSet(resultSet);
            }
        }
    }

    protected DefaultDataSet convertToDataSet(ResultSet resultSet) throws Exception {
        return convertToDataSet(resultSet, Collections.emptySet());
    }

    protected DefaultDataSet convertToDataSet(int batchId, ResultSet resultSet) throws Exception {
        return convertToDataSet(batchId, resultSet, Collections.emptySet());
    }

    protected DefaultDataSet convertToDataSet(ResultSet resultSet, Set<String> ignoreColumns) throws Exception {
        return convertToDataSet(0, resultSet, ignoreColumns);
    }

    protected DefaultDataSet convertToDataSet(int batchId, ResultSet resultSet, Set<String> ignoreColumns)
            throws Exception {
        int realColumnCount = resultSet.getMetaData().getColumnCount();
        List<DataColumn> columnOrder = new ArrayList<>();
        int columnCount = 0;
        for (int i = 1; i <= realColumnCount; i++) {
            String columnName = resultSet.getMetaData().getColumnName(i);
            int columnType = resultSet.getMetaData().getColumnType(i);
            int precision = resultSet.getMetaData().getPrecision(i);
            int scale = resultSet.getMetaData().getScale(i);
            if (ignoreColumns.stream().anyMatch(columnName::equalsIgnoreCase)) {
                continue;
            }
            columnCount += 1;
            columnOrder.add(new DefaultDataColumn(columnName, columnType, precision, scale));
        }
        List<List<Object>> results = new ArrayList<>();
        while (resultSet.next()) {
            List<Object> row = new ArrayList<>();
            for (DataColumn dataColumn : columnOrder) {
                int idx = resultSet.findColumn(dataColumn.getColumnName());
                Object object = resultSet.getObject(idx);
                // TODO: improve CLOB/BLOB handling
                Object mappedValue = databaseMigrationDataTypeMapperService.dataTypeMapper(object,
                        resultSet.getMetaData().getColumnType(idx));
                row.add(mappedValue);
            }
            results.add(row);
        }
        return new DefaultDataSet(batchId, columnCount, columnOrder, results);
    }

    @Override
    public void disableIndexesOfTable(String table) throws SQLException {
        final String disableIndexesScript;

        try {
            disableIndexesScript = getDisableIndexesScript(table);
        } catch (UnsupportedOperationException ignored) {
            LOG.debug("Disable index operation is not supported for '{}' database", getDatabaseProvider().getDbName());
            return;
        }

        try (Connection connection = getConnection();
                Statement stmt = connection.createStatement();
                ResultSet resultSet = stmt.executeQuery(disableIndexesScript)) {
            while (resultSet.next()) {
                runIndexQuery(resultSet);
            }
        }
    }

    @Override
    public void enableIndexesOfTable(String table) throws SQLException {
        final String enableIndexesScript;

        try {
            enableIndexesScript = getEnableIndexesScript(table);
        } catch (UnsupportedOperationException ignored) {
            LOG.debug("Enable index operation is not supported for '{}' database", getDatabaseProvider().getDbName());
            return;
        }

        try (Connection connection = getConnection();
                Statement stmt = connection.createStatement();
                ResultSet resultSet = stmt.executeQuery(enableIndexesScript)) {
            while (resultSet.next()) {
                runIndexQuery(resultSet);
            }
        }
    }

    @Override
    public void dropIndexesOfTable(String table) throws SQLException {
        final String dropIndexesScript;

        try {
            dropIndexesScript = getDropIndexesScript(table);
        } catch (UnsupportedOperationException ignored) {
            LOG.debug("Drop index operation is not supported for '{}' database", getDatabaseProvider().getDbName());
            return;
        }

        try (Connection connection = getConnection();
                Statement stmt = connection.createStatement();
                ResultSet resultSet = stmt.executeQuery(dropIndexesScript)) {
            while (resultSet.next()) {
                runIndexQuery(resultSet);
            }
        }
    }

    private void runIndexQuery(ResultSet resultSet) throws SQLException {
        String q = resultSet.getString(1);
        LOG.debug("Running query: {}", q);
        executeUpdateAndCommit(q);
    }

    protected String getDisableIndexesScript(String table) {
        throw new UnsupportedOperationException();
    }

    protected String getEnableIndexesScript(String table) {
        throw new UnsupportedOperationException();
    }

    protected String getDropIndexesScript(String table) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Platform asPlatform() {
        return asPlatform(false);
    }

    @Override
    public Platform asPlatform(boolean reload) {
        // TODO all properties to be set and check
        if (this.platform == null || reload) {
            final PropertiesLoader propertiesLoader = new TenantPropertiesLoader(Registry.getMasterTenant());
            final DatabaseSettings databaseSettings = new DatabaseSettings(getDatabaseProvider(),
                    getDataSourceConfiguration().getConnectionString(), getDataSourceConfiguration().getDriver(),
                    getDataSourceConfiguration().getUserName(), getDataSourceConfiguration().getPassword(),
                    getDataSourceConfiguration().getTablePrefix(), propertiesLoader, ";");
            this.platform = createPlatform(databaseSettings, getDataSource());
            addCustomPlatformTypeMapping(this.platform);
        }
        return this.platform;
    }

    protected Platform createPlatform(DatabaseSettings databaseSettings, DataSource dataSource) {
        return HybrisPlatformFactory.createInstance(databaseSettings, dataSource);
    }

    protected void addCustomPlatformTypeMapping(Platform platform) {
    }

    @Override
    public Database asDatabase() {
        return asDatabase(false);
    }

    @Override
    public Database asDatabase(boolean reload) {
        if (this.database == null || reload) {
            this.database = getDatabase(reload);
        }
        return this.database;
    }

    protected Database getDatabase(boolean reload) {
        String schema = getDataSourceConfiguration().getSchema();
        return asPlatform(reload).readModelFromDatabase(getDataSourceConfiguration().getProfile(), null, schema, null);
    }

    @Override
    public Set<String> getAllTableNames() throws SQLException {
        Set<String> allTableNames = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        String allTableNamesQuery = createAllTableNamesQuery();
        try (Connection connection = getConnection();
                Statement stmt = connection.createStatement();
                ResultSet resultSet = stmt.executeQuery(allTableNamesQuery)) {
            while (resultSet.next()) {
                String tableName = resultSet.getString(1);
                if (!StringUtils.startsWithIgnoreCase(tableName, CommercedbsyncConstants.MIGRATION_TABLESPREFIX)) {
                    allTableNames.add(resultSet.getString(1));
                }
            }
        }
        return allTableNames;
    }

    @Override
    public Set<TypeSystemTable> getAllTypeSystemTables() throws SQLException {
        if (StringUtils.isEmpty(getDataSourceConfiguration().getTypeSystemName())) {
            throw new RuntimeException("No type system name specified. Check the properties");
        }
        String tablePrefix = getDataSourceConfiguration().getTablePrefix();
        String yDeploymentsTable = StringUtils.defaultIfBlank(tablePrefix, "")
                + CommercedbsyncConstants.DEPLOYMENTS_TABLE;
        Set<String> allTableNames = getAllTableNames();
        if (!allTableNames.contains(yDeploymentsTable)) {
            return Collections.emptySet();
        }
        String allTypeSystemTablesQuery = String.format(
                "SELECT * FROM %s WHERE Typecode IS NOT NULL AND TableName IS NOT NULL AND TypeSystemName = '%s'",
                yDeploymentsTable, getDataSourceConfiguration().getTypeSystemName());
        Set<TypeSystemTable> allTypeSystemTables = new HashSet<>();
        try (Connection connection = getConnection();
                Statement stmt = connection.createStatement();
                ResultSet resultSet = stmt.executeQuery(allTypeSystemTablesQuery)) {
            while (resultSet.next()) {
                TypeSystemTable typeSystemTable = new TypeSystemTable();
                String name = resultSet.getString("Name");
                String tableName = resultSet.getString("TableName");
                typeSystemTable.setTypeCode(resultSet.getString("Typecode"));
                typeSystemTable.setTableName(tableName);
                typeSystemTable.setName(name);
                typeSystemTable.setTypeSystemName(resultSet.getString("TypeSystemName"));
                if (hasAuditTable(resultSet, yDeploymentsTable)) {
                    typeSystemTable.setAuditTableName(resultSet.getString("AuditTableName"));
                }
                typeSystemTable.setPropsTableName(resultSet.getString("PropsTableName"));
                typeSystemTable.setTypeSystemSuffix(detectTypeSystemSuffix(tableName, name));
                typeSystemTable.setTypeSystemRelatedTable(PersistenceInformation.isTypeSystemRelatedDeployment(name));
                allTypeSystemTables.add(typeSystemTable);
            }
        }
        return allTypeSystemTables;
    }

    private Boolean hasAuditTable(final ResultSet resultSet, final String yDeploymentsTable) {
        try {
            return resultSet.findColumn("AuditTableName") > 0;
        } catch (SQLException ignored) {
            LOG.debug("No column with name 'AuditTableName' found in table {}", yDeploymentsTable);
            return false;
        }
    }

    private String detectTypeSystemSuffix(String tableName, String name) {
        if (PersistenceInformation.isTypeSystemRelatedDeployment(name)) {
            return getDataSourceConfiguration().getTypeSystemSuffix();
        }
        return StringUtils.EMPTY;
    }

    @Override
    public boolean isAuditTable(String table) throws Exception {
        String tablePrefix = getDataSourceConfiguration().getTablePrefix();
        String query = String.format("SELECT count(*) from %s%s WHERE AuditTableName = ? OR AuditTableName = ?",
                StringUtils.defaultIfBlank(tablePrefix, ""), CommercedbsyncConstants.DEPLOYMENTS_TABLE);
        try (Connection connection = getConnection(); PreparedStatement stmt = connection.prepareStatement(query)) {
            stmt.setObject(1, StringUtils.removeStartIgnoreCase(table, tablePrefix));
            stmt.setObject(2, table);
            try (ResultSet rs = stmt.executeQuery()) {
                boolean isAudit = false;
                if (rs.next()) {
                    isAudit = rs.getInt(1) > 0;
                }
                return isAudit;
            }
        } catch (SQLException ignored) {
            return false;
        }
    }

    protected abstract String createAllTableNamesQuery();

    @Override
    public Set<String> getAllColumnNames(String table) throws SQLException {
        String allColumnNamesQuery = createAllColumnNamesQuery(table);
        Set<String> allColumnNames = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        try (Connection connection = getConnection();
                Statement stmt = connection.createStatement();
                ResultSet resultSet = stmt.executeQuery(allColumnNamesQuery)) {
            while (resultSet.next()) {
                allColumnNames.add(resultSet.getString(1));
            }
        }
        return allColumnNames;
    }

    protected abstract String createAllColumnNamesQuery(String table);

    @Override
    public DataSet getBatchWithoutIdentifier(OffsetQueryDefinition queryDefinition) throws Exception {
        return getBatchWithoutIdentifier(queryDefinition, null);
    }

    @Override
    public DataSet getBatchWithoutIdentifier(OffsetQueryDefinition queryDefinition, Instant time) throws Exception {
        // get batches with modifiedts >= configured time for incremental migration
        List<String> conditionsList = new ArrayList<>(1);
        processDefaultConditions(queryDefinition.getTable(), conditionsList);
        if (time != null) {
            conditionsList.add("modifiedts > ?");
        }
        String[] conditions = null;
        if (!conditionsList.isEmpty()) {
            conditions = conditionsList.toArray(new String[conditionsList.size()]);
            if (LOG.isDebugEnabled()) {
                LOG.debug("Batch query conditions {}", Arrays.toString(conditions));
            }
        }
        LOG.debug("Batch query table: {}, offset: {}, batchSize: {}, orderByColumns: {}", queryDefinition.getTable(),
                queryDefinition.getOffset(), queryDefinition.getBatchSize(), queryDefinition.getOrderByColumns());
        try (Connection connection = getConnection();
                PreparedStatement stmt = connection
                        .prepareStatement(buildOffsetBatchQuery(queryDefinition, conditions))) {
            stmt.setFetchSize(Long.valueOf(queryDefinition.getBatchSize()).intValue());
            int paramIdx = 0;
            if (time != null) {
                stmt.setTimestamp(++paramIdx, Timestamp.from(time));
            }

            if (hasParameterizedOffsetBatchQuery()) {
                stmt.setLong(++paramIdx, queryDefinition.getOffset());
                stmt.setLong(++paramIdx, queryDefinition.getBatchSize());
            }
            ResultSet resultSet = stmt.executeQuery();
            return convertToBatchDataSet(queryDefinition.getBatchId(), resultSet);
        }
    }

    @Override
    public DataSet getBatchOrderedByColumn(SeekQueryDefinition queryDefinition) throws Exception {
        return getBatchOrderedByColumn(queryDefinition, null);
    }

    @Override
    public DataSet getBatchOrderedByColumn(SeekQueryDefinition queryDefinition, Instant time) throws Exception {
        // get batches with modifiedts >= configured time for incremental migration
        List<String> conditionsList = new ArrayList<>(2);
        processDefaultConditions(queryDefinition.getTable(), conditionsList);
        int conditionIndex = 1;
        int timeConditionIndex = 0;
        if (time != null) {
            conditionsList.add("modifiedts > ?");
            timeConditionIndex = conditionIndex++;
        }
        int lastColumnConditionIndex = 0;
        if (queryDefinition.getLastColumnValue() != null) {
            conditionsList.add(String.format(getLastValueCondition(), queryDefinition.getColumn()));
            lastColumnConditionIndex = conditionIndex++;
        }
        int nextColumnConditionIndex = 0;
        if (queryDefinition.getNextColumnValue() != null) {
            conditionsList.add(String.format(getNextValueCondition(), queryDefinition.getColumn()));
            nextColumnConditionIndex = conditionIndex++;
        }
        String[] conditions = null;
        if (conditionsList.size() > 0) {
            conditions = conditionsList.toArray(new String[conditionsList.size()]);
        }
        try (Connection connection = getConnection();
                PreparedStatement stmt = connection
                        .prepareStatement(buildValueBatchQuery(queryDefinition, conditions))) {
            stmt.setFetchSize(Long.valueOf(queryDefinition.getBatchSize()).intValue());
            if (timeConditionIndex > 0) {
                stmt.setTimestamp(timeConditionIndex, Timestamp.from(time));
            }
            if (lastColumnConditionIndex > 0) {
                stmt.setObject(lastColumnConditionIndex, queryDefinition.getLastColumnValue());
            }
            if (nextColumnConditionIndex > 0) {
                stmt.setObject(nextColumnConditionIndex, queryDefinition.getNextColumnValue());
            }
            ResultSet resultSet = stmt.executeQuery();
            return convertToBatchDataSet(queryDefinition.getBatchId(), resultSet);
        }
    }

    protected String getLastValueCondition() {
        return "%s >= ?";
    }

    protected String getNextValueCondition() {
        return "%s < ?";
    }

    @Override
    public DataSet getBatchMarkersOrderedByColumn(MarkersQueryDefinition queryDefinition) throws Exception {
        return getBatchMarkersOrderedByColumn(queryDefinition, null);
    }

    @Override
    public DataSet getBatchMarkersOrderedByColumn(MarkersQueryDefinition queryDefinition, Instant time)
            throws Exception {
        // get batches with modifiedts >= configured time for incremental migration
        List<String> conditionsList = new ArrayList<>(2);
        processDefaultConditions(queryDefinition.getTable(), conditionsList);
        if (time != null) {
            conditionsList.add("modifiedts > ?");
        }
        String[] conditions = null;
        if (!conditionsList.isEmpty()) {
            conditions = conditionsList.toArray(new String[conditionsList.size()]);
        }
        try (Connection connection = getConnection();
                PreparedStatement stmt = connection
                        .prepareStatement(buildBatchMarkersQuery(queryDefinition, conditions))) {
            stmt.setFetchSize(Long.valueOf(queryDefinition.getBatchSize()).intValue());
            int paramIdx = 0;
            if (time != null) {
                stmt.setTimestamp(++paramIdx, Timestamp.from(time));
            }
            if (hasParameterizedBatchMarkersQuery()) {
                stmt.setLong(++paramIdx, queryDefinition.getBatchSize());
            }

            ResultSet resultSet = stmt.executeQuery();
            return convertToBatchDataSet(0, resultSet);
        }
    }

    @Override
    public DataSet getUniqueColumns(String table) throws Exception {
        try (Connection connection = getConnection(); Statement stmt = connection.createStatement()) {
            ResultSet resultSet = stmt.executeQuery(createUniqueColumnsQuery(table));
            return convertToDataSet(resultSet);
        }
    }

    protected abstract String buildOffsetBatchQuery(OffsetQueryDefinition queryDefinition, String... conditions);

    protected boolean hasParameterizedOffsetBatchQuery() {
        return false;
    }

    protected abstract String buildValueBatchQuery(SeekQueryDefinition queryDefinition, String... conditions);

    protected abstract String buildBatchMarkersQuery(MarkersQueryDefinition queryDefinition, String... conditions);

    protected boolean hasParameterizedBatchMarkersQuery() {
        return false;
    }

    protected abstract String createUniqueColumnsQuery(String tableName);

    protected void processDefaultConditions(String table, List<String> conditionsList) {
        String tsCondition = getTsCondition(table);
        if (StringUtils.isNotEmpty(tsCondition)) {
            conditionsList.add(tsCondition);
        }
    }

    private String getTsCondition(String table) {
        Objects.requireNonNull(table);
        if (table.toLowerCase().endsWith(CommercedbsyncConstants.DEPLOYMENTS_TABLE)) {
            return String.format("TypeSystemName = '%s'", getDataSourceConfiguration().getTypeSystemName());
        }
        return null;
    }

    protected String expandConditions(String[] conditions) {
        if (conditions == null || conditions.length == 0) {
            return "1=1";
        } else {
            return Joiner.on(" AND ").join(conditions);
        }
    }

    protected DataSet convertToBatchDataSet(int batchId, ResultSet resultSet) throws Exception {
        return convertToDataSet(batchId, resultSet);
    }

    @Override
    public boolean validateConnection() throws SQLException {
        try (Connection connection = getConnection()) {
            return connection.isValid(120);
        }
    }

    @Override
    public Set<String> getAllViewNames() throws SQLException {
        Set<String> allViewNames = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        try (Connection connection = getConnection()) {
            DatabaseMetaData meta = connection.getMetaData();
            ResultSet resultSet = meta.getTables(null, null, "%", new String[]{"VIEW"});
            while (resultSet.next()) {
                String tableName = resultSet.getString("TABLE_NAME");
                if (!StringUtils.startsWithIgnoreCase(tableName, CommercedbsyncConstants.MIGRATION_TABLESPREFIX)) {
                    allViewNames.add(tableName);
                }
            }
        }
        return allViewNames;
    }

    public JDBCQueriesStore getJdbcQueriesStore() {
        // the store will have no entries if dataSourceConfiguration.isLogSql() is false
        return jdbcQueriesStore;
    }

    protected Map<String, String> getLocationMap() {
        final String connectionString = getDataSourceConfiguration().getConnectionString();
        int endIndex = connectionString.indexOf('?');
        String newConnectionString = connectionString.substring(endIndex + 1);
        List<String> entries = getTokensWithCollection(newConnectionString, "&");

        final Map<String, String> locationMap = entries.stream().map(s -> s.split("="))
                .collect(Collectors.toMap(s -> s[0], s -> s[1]));
        return locationMap;
    }

    @Override
    public void clearJdbcQueriesStore() {
        jdbcQueriesStore.clear();
    }

    public List<String> getTokensWithCollection(final String str, final String delimiter) {
        return Collections.list(new StringTokenizer(str, delimiter)).stream().map(token -> (String) token)
                .collect(Collectors.toList());
    }

    protected String getLpTableName(String tableName) {
        return StringUtils.removeEndIgnoreCase(tableName, LP_SUFFIX);
    }

    protected abstract String getBulkInsertStatementParamList(List<String> columnsToCopy,
            List<String> columnsToCopyValues);

    protected abstract String getBulkUpdateStatementParamList(List<String> columnsToCopy,
            List<String> columnsToCopyValues, List<String> upsertIDs);
}
