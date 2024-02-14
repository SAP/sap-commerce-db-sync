/*
 *  Copyright: 2023 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.repository;

import com.sap.cx.boosters.commercedbsync.dataset.DataSet;
import com.sap.cx.boosters.commercedbsync.logging.JDBCQueriesStore;
import com.sap.cx.boosters.commercedbsync.profile.DataSourceConfiguration;
import de.hybris.bootstrap.ddl.DataBaseProvider;
import org.apache.ddlutils.Platform;
import org.apache.ddlutils.model.Database;
import com.sap.cx.boosters.commercedbsync.MarkersQueryDefinition;
import com.sap.cx.boosters.commercedbsync.OffsetQueryDefinition;
import com.sap.cx.boosters.commercedbsync.SeekQueryDefinition;
import com.sap.cx.boosters.commercedbsync.TypeSystemTable;
import org.springframework.core.io.Resource;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Instant;
import java.util.List;
import java.util.Set;

/**
 *
 */
public interface DataRepository {

    String getDatabaseTimezone();

    Database asDatabase();

    Database asDatabase(boolean reload);

    Set<String> getAllTableNames() throws Exception;

    Set<String> getAllViewNames() throws SQLException;

    Set<TypeSystemTable> getAllTypeSystemTables() throws Exception;

    boolean isAuditTable(String table) throws Exception;

    Set<String> getAllColumnNames(String table) throws Exception;

    DataSet getBatchWithoutIdentifier(OffsetQueryDefinition queryDefinition) throws Exception;

    DataSet getBatchWithoutIdentifier(OffsetQueryDefinition queryDefinition, Instant time) throws Exception;

    DataSet getBatchOrderedByColumn(SeekQueryDefinition queryDefinition) throws Exception;

    DataSet getBatchOrderedByColumn(SeekQueryDefinition queryDefinition, Instant time) throws Exception;

    DataSet getBatchMarkersOrderedByColumn(MarkersQueryDefinition queryDefinition) throws Exception;

    long getRowCount(String table) throws Exception;

    long getRowCountModifiedAfter(String table, Instant time, boolean isDeletionEnabled,
            boolean lpTableMigrationEnabled) throws SQLException;

    long getRowCountModifiedAfter(String table, Instant time) throws SQLException;

    DataSet getAll(String table) throws Exception;

    DataSet getAllModifiedAfter(String table, Instant time) throws Exception;

    DataSourceConfiguration getDataSourceConfiguration();

    int executeUpdateAndCommit(String updateStatement) throws Exception;

    int executeUpdateAndCommitOnPrimary(String updateStatement) throws Exception;

    void runSqlScript(final Resource resource);

    void runSqlScriptOnPrimary(final Resource resource);

    float getDatabaseUtilization();

    int truncateTable(String table) throws Exception;

    void disableIndexesOfTable(String table) throws Exception;

    void enableIndexesOfTable(String table) throws SQLException;

    void dropIndexesOfTable(String table) throws SQLException;

    Platform asPlatform();

    Platform asPlatform(boolean reload);

    DataBaseProvider getDatabaseProvider();

    Connection getConnection() throws Exception;

    DataSource getDataSource();

    DataSource getDataSourcePrimary();

    DataSet getBatchMarkersOrderedByColumn(MarkersQueryDefinition queryDefinition, Instant time) throws Exception;

    DataSet getUniqueColumns(String table) throws Exception;

    boolean validateConnection() throws Exception;

    /**
     * Get the store of JDBC queries associated with the datasouce
     *
     * @return store of JDBC queries associated with the datasouce
     */
    JDBCQueriesStore getJdbcQueriesStore();

    /**
     * Clear the store of JDBC queries from all the entries it currently contains
     */
    void clearJdbcQueriesStore();

    String buildBulkUpsertStatement(String table, List<String> columnsToCopy, List<String> upsertIDs);
}
