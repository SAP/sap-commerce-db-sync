/*
 *  Copyright: 2023 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.profile.impl;

import com.sap.cx.boosters.commercedbsync.profile.DataSourceConfiguration;
import com.sap.cx.boosters.commercedbsync.profile.DataSourceConfigurationFactory;
import de.hybris.platform.servicelayer.config.ConfigurationService;

public class DefaultDataSourceConfigurationFactory implements DataSourceConfigurationFactory {

    private final ConfigurationService configurationService;

    public DefaultDataSourceConfigurationFactory(ConfigurationService configurationService) {
        this.configurationService = configurationService;
    }

    @Override
    public DataSourceConfiguration create(String profile) {
        return new DefaultDataSourceConfiguration(configurationService.getConfiguration(), profile);
    }
}
