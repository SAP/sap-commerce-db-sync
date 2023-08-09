/*
 *  Copyright: 2023 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package de.hybris.platform.core;

import de.hybris.bootstrap.ddl.PropertiesLoader;

import java.util.Objects;

public class TenantPropertiesLoader implements PropertiesLoader {
    private final Tenant tenant;

    public TenantPropertiesLoader(final Tenant tenant) {
        Objects.requireNonNull(tenant);
        this.tenant = tenant;
    }

    @Override
    public String getProperty(final String key) {
        return tenant.getConfig().getParameter(key);
    }

    @Override
    public String getProperty(final String key, final String defaultValue) {
        return tenant.getConfig().getString(key, defaultValue);
    }
}
