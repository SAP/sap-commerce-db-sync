/*
 *  Copyright: 2026 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.datasource.impl;

import com.sap.cx.boosters.commercedbsync.profile.DataSourceConfiguration;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.springframework.util.ReflectionUtils;

import java.util.Map;

import javax.sql.DataSource;

public class DefaultMigrationDataSourceFactory extends AbstractMigrationDataSourceFactory {

    @Override
    public DataSource create(final DataSourceConfiguration dataSourceConfiguration) {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(dataSourceConfiguration.getConnectionString());
        config.setDriverClassName(dataSourceConfiguration.getDriver());
        config.setUsername(dataSourceConfiguration.getUserName());
        config.setPassword(dataSourceConfiguration.getPassword());
        config.setMaximumPoolSize(dataSourceConfiguration.getMaxActive());
        config.setMinimumIdle(dataSourceConfiguration.getMinIdle());
        config.setRegisterMbeans(true);
        config.setMaxLifetime(dataSourceConfiguration.getMaxLifetime());
        config.setIdleTimeout(dataSourceConfiguration.getIdleTimeout());
        // TODO check conflict with old HikariCP (< 3.4.0) causing method not found
        // exception
        if (canSetKeepaliveTime()) {
            config.setKeepaliveTime(dataSourceConfiguration.getKeepaliveTime());
        }
        config.setSchema(dataSourceConfiguration.getSchema());
        config.setDataSourceProperties(dataSourceConfiguration.getDriverProperties());
        return new HikariDataSource(config);
    }

    @Override
    public DataSource create(final Map<String, Object> dataSourceConfigurationMap) {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl((String) dataSourceConfigurationMap.get("connection.url"));
        config.setDriverClassName((String) dataSourceConfigurationMap.get("driver"));
        config.setUsername((String) dataSourceConfigurationMap.get("username"));
        config.setPassword((String) dataSourceConfigurationMap.get("password"));
        config.setMaximumPoolSize((Integer) dataSourceConfigurationMap.get("pool.size.max"));
        config.setMinimumIdle((Integer) dataSourceConfigurationMap.get("pool.size.idle.min"));
        config.setRegisterMbeans((Boolean) dataSourceConfigurationMap.get("registerMbeans"));
        return new HikariDataSource(config);
    }

    private boolean canSetKeepaliveTime() {
        return ReflectionUtils.findMethod(HikariConfig.class, "setKeepaliveTime") != null;
    }
}
