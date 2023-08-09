/*
 *  Copyright: 2023 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.profile.impl;

import com.sap.cx.boosters.commercedbsync.profile.DataSourceConfiguration;

public class InvalidDataSourceConfigurationException extends RuntimeException {
    public InvalidDataSourceConfigurationException(String message, DataSourceConfiguration dataSourceConfiguration) {
        super(message + ": " + dataSourceConfiguration);
    }

}
