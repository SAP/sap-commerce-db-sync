/*
 *  Copyright: 2023 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.profile;

/**
 * Contains a DataSource Configuration
 */
public interface DataSourceConfiguration {
    String getProfile();

    String getDriver();

    String getConnectionString();

    String getConnectionStringPrimary();

    String getUserName();

    String getPassword();

    String getSchema();

    String getTypeSystemName();

    String getTypeSystemSuffix();

    String getCatalog();

    String getTablePrefix();

    int getMaxActive();

    int getMaxIdle();

    int getMinIdle();

    boolean isRemoveAbandoned();
}
