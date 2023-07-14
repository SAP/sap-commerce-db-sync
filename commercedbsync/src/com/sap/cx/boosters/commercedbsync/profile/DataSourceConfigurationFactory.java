/*
 *  Copyright: 2023 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.profile;

/**
 * Factory to create datasource configurations based on profiles
 */
public interface DataSourceConfigurationFactory {
    DataSourceConfiguration create(String profile);
}
