/*
 *  Copyright: 2022 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.datasource;

import com.sap.cx.boosters.commercedbsync.profile.DataSourceConfiguration;

import javax.sql.DataSource;

/**
 * Factory to create the DataSources used for Migration
 */
public interface MigrationDataSourceFactory {
    DataSource create(DataSourceConfiguration dataSourceConfiguration);
}
