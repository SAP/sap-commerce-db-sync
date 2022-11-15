/*
 *  Copyright: 2022 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.datasource.impl;

import com.sap.cx.boosters.commercedbsync.profile.DataSourceConfiguration;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import javax.sql.DataSource;

public class DefaultMigrationDataSourceFactory extends AbstractMigrationDataSourceFactory {

    //TODO: resource leak: DataSources are never closed
    @Override
    public DataSource create(DataSourceConfiguration dataSourceConfiguration) {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(dataSourceConfiguration.getConnectionString());
        config.setDriverClassName(dataSourceConfiguration.getDriver());
        config.setUsername(dataSourceConfiguration.getUserName());
        config.setPassword(dataSourceConfiguration.getPassword());
        config.setMaximumPoolSize(dataSourceConfiguration.getMaxActive());
        config.setMinimumIdle(dataSourceConfiguration.getMinIdle());
        config.setRegisterMbeans(true);
        return new HikariDataSource(config);
    }

}
