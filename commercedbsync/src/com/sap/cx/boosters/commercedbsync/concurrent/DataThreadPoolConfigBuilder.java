/*
 *  Copyright: 2023 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.concurrent;

import com.sap.cx.boosters.commercedbsync.DataThreadPoolConfig;
import com.sap.cx.boosters.commercedbsync.context.MigrationContext;

public class DataThreadPoolConfigBuilder {

    private final DataThreadPoolConfig config;

    public DataThreadPoolConfigBuilder(MigrationContext context) {
        config = new DataThreadPoolConfig();
    }

    public DataThreadPoolConfigBuilder withPoolSize(int poolSize) {
        config.setPoolSize(poolSize);
        return this;
    }

    public DataThreadPoolConfig build() {
        return config;
    }
}
