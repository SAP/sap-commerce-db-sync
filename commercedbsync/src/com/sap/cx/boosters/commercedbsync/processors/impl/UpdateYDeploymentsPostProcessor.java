/*
 *  Copyright: 2023 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.processors.impl;

import com.sap.cx.boosters.commercedbsync.context.CopyContext;
import com.sap.cx.boosters.commercedbsync.processors.MigrationPostProcessor;
import com.sap.cx.boosters.commercedbsync.provider.CopyItemProvider;
import com.sap.cx.boosters.commercedbsync.repository.DataRepository;
import com.sap.cx.boosters.commercedbsync.service.DatabaseCopyTaskRepository;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.sap.cx.boosters.commercedbsync.constants.CommercedbsyncConstants.DEPLOYMENTS_TABLE;
import static com.sap.cx.boosters.commercedbsync.provider.CopyItemProvider.TYPE_SYSTEM_PROPS_TABLE;

/**
 * <b>Deployments table adjustment post processor</b> <br/>
 * <br/>
 * In case migrated type system suffix
 * (`migration.ds.source.db.typesystemsuffix`) is different from target suffix
 * (`migration.ds.target.db.typesystemsuffix`), it is mandatory to update
 * references in `ydeployments` entires, to point to proper database tables,
 * instead of original values migrated from source system
 */
public class UpdateYDeploymentsPostProcessor implements MigrationPostProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(UpdateYDeploymentsPostProcessor.class.getName());

    private static final String DEFAULT_TYPE_SYSTEM_NAME = "DEFAULT";

    private static final String ADJUST_DATA_TABLE_STATEMENT = "UPDATE %s SET TableName = ? WHERE LOWER(TableName) = ? AND LOWER(TypeSystemName) = ?";
    private static final String ADJUST_PROPS_TABLE_STATEMENT = "UPDATE %s SET PropsTableName = ? WHERE LOWER(PropsTableName) = ? AND LOWER(TypeSystemName) = ?";
    private static final String GET_TABLE_STATEMENT = "SELECT DISTINCT(PropsTableName) FROM %s WHERE LOWER(PropsTableName) = ?";

    private static final List<String> TYPE_SYSTEM_DATA_TABLES = Stream.of(CopyItemProvider.TYPE_SYSTEM_RELATED_TYPES)
            .filter(t -> !TYPE_SYSTEM_PROPS_TABLE.equalsIgnoreCase(t)).collect(Collectors.toList());

    private DatabaseCopyTaskRepository databaseCopyTaskRepository;

    @Override
    public void process(final CopyContext context) {
        final DataRepository sourceRepository = context.getMigrationContext().getDataSourceRepository();
        final DataRepository targetRepository = context.getMigrationContext().getDataTargetRepository();

        boolean useUpperCaseTableName = false;

        try (Connection connection = targetRepository.getConnection()) {
            try (PreparedStatement statement = connection.prepareStatement(
                    String.format(GET_TABLE_STATEMENT, getFullDeploymentsTableName(targetRepository, false)))) {
                statement.setString(1, getTypeSystemTableName(sourceRepository, TYPE_SYSTEM_PROPS_TABLE, false));

                try (ResultSet rs = statement.executeQuery()) {
                    if (rs.next()) {
                        useUpperCaseTableName = getTypeSystemTableName(sourceRepository, TYPE_SYSTEM_PROPS_TABLE, true)
                                .equals(rs.getString(1));
                    }
                }
            } catch (SQLException e) {
                LOG.error("Error fetching data for post processor (SQLException) ", e);
            }

            final String deploymentsTableName = getFullDeploymentsTableName(targetRepository, useUpperCaseTableName);
            final String typeSystemName = getTypeSystemName(targetRepository);

            try (PreparedStatement statement = connection
                    .prepareStatement(String.format(ADJUST_PROPS_TABLE_STATEMENT, deploymentsTableName))) {
                statement.setString(1,
                        getTypeSystemTableName(targetRepository, TYPE_SYSTEM_PROPS_TABLE, useUpperCaseTableName));
                statement.setString(2, getTypeSystemTableName(sourceRepository, TYPE_SYSTEM_PROPS_TABLE, false));
                statement.setString(3, typeSystemName.toLowerCase());

                final int updated = statement.executeUpdate();

                if (updated > 0) {
                    LOG.debug("Adjusted `{}` table values for `{}` entries in type system: {}", deploymentsTableName,
                            TYPE_SYSTEM_PROPS_TABLE, typeSystemName);
                }
            } catch (SQLException e) {
                LOG.error("Error while executing post processor (SQLException) ", e);
            }

            try (PreparedStatement statement = connection
                    .prepareStatement(String.format(ADJUST_DATA_TABLE_STATEMENT, deploymentsTableName))) {
                int updated;

                for (String typeSystemDataTable : TYPE_SYSTEM_DATA_TABLES) {
                    statement.setString(1,
                            getTypeSystemTableName(targetRepository, typeSystemDataTable, useUpperCaseTableName));
                    statement.setString(2, getTypeSystemTableName(sourceRepository, typeSystemDataTable, false));
                    statement.setString(3, typeSystemName.toLowerCase());

                    updated = statement.executeUpdate();

                    if (updated > 0) {
                        LOG.debug("Adjusted `{}` table values for `{}` entries in type system: {}",
                                deploymentsTableName, typeSystemDataTable, typeSystemName);
                    }
                }
            } catch (SQLException e) {
                LOG.error("Error fetching data for post processor (SQLException) ", e);
            }

            LOG.info("Finished `{}` table adjustments for type system: {}", deploymentsTableName, typeSystemName);
        } catch (Exception e) {
            LOG.error("Error executing post processor", e);
        }
    }

    private String getTypeSystemTableName(DataRepository repository, String tableName, boolean upperCase) {
        tableName += StringUtils.trimToEmpty(repository.getDataSourceConfiguration().getTypeSystemSuffix());

        return upperCase ? tableName.toUpperCase() : tableName.toLowerCase();
    }

    private String getFullDeploymentsTableName(DataRepository repository, boolean upperCase) {
        final String tableName = StringUtils.trimToEmpty(repository.getDataSourceConfiguration().getTablePrefix())
                + DEPLOYMENTS_TABLE;

        return upperCase ? tableName.toUpperCase() : tableName.toLowerCase();
    }

    private String getTypeSystemName(DataRepository repository) {
        return StringUtils.defaultString(repository.getDataSourceConfiguration().getTypeSystemName(),
                DEFAULT_TYPE_SYSTEM_NAME);
    }

    @Override
    public boolean shouldExecute(CopyContext context) {
        final String srcTypeSystemSuffix = context.getMigrationContext().getDataSourceRepository()
                .getDataSourceConfiguration().getTypeSystemSuffix();
        final String tgtTypeSystemSuffix = context.getMigrationContext().getDataTargetRepository()
                .getDataSourceConfiguration().getTypeSystemSuffix();

        try {
            return !srcTypeSystemSuffix.equals(tgtTypeSystemSuffix)
                    && databaseCopyTaskRepository.getAllTasks(context).stream().anyMatch(
                            task -> StringUtils.containsIgnoreCase(task.getTargettablename(), DEPLOYMENTS_TABLE));
        } catch (Exception e) {
            LOG.warn("Unable to check post processor execution condition", e);
        }

        return false;
    }

    public void setDatabaseCopyTaskRepository(final DatabaseCopyTaskRepository databaseCopyTaskRepository) {
        this.databaseCopyTaskRepository = databaseCopyTaskRepository;
    }
}
