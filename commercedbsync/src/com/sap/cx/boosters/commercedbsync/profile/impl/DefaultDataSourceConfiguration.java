/*
 *  Copyright: 2022 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.profile.impl;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.StringUtils;
import com.sap.cx.boosters.commercedbsync.profile.DataSourceConfiguration;

/**
 * Contains the JDBC DataSource Configuration
 */
public class DefaultDataSourceConfiguration implements DataSourceConfiguration {

    private String profile;
    private Configuration configuration;
    private String driver;
    private String connectionString;
    private String userName;
    private String password;
    private String schema;
    private String catalog;
    private String tablePrefix;
    private String typeSystemName;
    private String typeSystemSuffix;
    private int maxActive;
    private int maxIdle;
    private int minIdle;
    private boolean removedAbandoned;

    public DefaultDataSourceConfiguration(Configuration configuration, String profile) {
        this.profile = profile;
        this.configuration = configuration;
        this.load(configuration, profile);
    }

    @Override
    public String getProfile() {
        return profile;
    }

    @Override
    public String getDriver() {
        return driver;
    }

    @Override
    public String getConnectionString() {
        return connectionString;
    }

    @Override
    public String getUserName() {
        return userName;
    }

    @Override
    public String getPassword() {
        return password;
    }

    @Override
    public String getSchema() {
        return schema;
    }

    @Override
    public String getTypeSystemName() {
        this.typeSystemName = getProfileProperty(profile, configuration, "db.typesystemname");
        return typeSystemName;
    }

    @Override
    public String getTypeSystemSuffix() {
        this.typeSystemSuffix = getProfileProperty(profile, configuration, "db.typesystemsuffix");
        return typeSystemSuffix;
    }

    @Override
    public String getCatalog() {
        return catalog;
    }

    @Override
    public String getTablePrefix() {
        return tablePrefix;
    }

    @Override
    public int getMaxActive() {
        return maxActive;
    }

    @Override
    public int getMaxIdle() {
        return maxIdle;
    }

    @Override
    public int getMinIdle() {
        return minIdle;
    }

    @Override
    public boolean isRemoveAbandoned() {
        return removedAbandoned;
    }

    protected void load(Configuration configuration, String profile) {
        this.driver = getProfileProperty(profile, configuration, "db.driver");
        this.connectionString = getProfileProperty(profile, configuration, "db.url");
        this.userName = getProfileProperty(profile, configuration, "db.username");
        this.password = getProfileProperty(profile, configuration, "db.password");
        this.schema = getProfileProperty(profile, configuration, "db.schema");
        this.catalog = getProfileProperty(profile, configuration, "db.catalog");
        this.tablePrefix = getProfileProperty(profile, configuration, "db.tableprefix");
        this.typeSystemName = getProfileProperty(profile, configuration, "db.typesystemname");
        this.typeSystemSuffix = getProfileProperty(profile, configuration, "db.typesystemsuffix");
        this.maxActive = parseInt(getProfileProperty(profile, configuration, "db.connection.pool.size.active.max"));
        this.maxIdle = parseInt(getProfileProperty(profile, configuration, "db.connection.pool.size.idle.max"));
        this.minIdle = parseInt(getProfileProperty(profile, configuration, "db.connection.pool.size.idle.min"));
        this.removedAbandoned = Boolean.parseBoolean(getProfileProperty(profile, configuration, "db.connection.removeabandoned"));
    }

    protected String getNormalProperty(Configuration configuration, String key) {
        return checkProperty(configuration.getString(key), key);
    }

    protected int parseInt(String value) {
        if (StringUtils.isEmpty(value)) {
            return 0;
        } else {
            return Integer.parseInt(value);
        }
    }

    protected String getProfileProperty(String profile, Configuration configuration, String key) {
        String profilePropertyKey = createProfilePropertyKey(key, profile);
        String property = configuration.getString(profilePropertyKey);
        if (StringUtils.startsWith(property, "${")) {
            property = configuration.getString(StringUtils.substringBetween(property, "{", "}"));
        }
        return checkProperty(property, profilePropertyKey);
    }

    protected String checkProperty(String property, String key) {
        if (property != null) {
            return property;
        } else {
            throw new IllegalArgumentException(String.format(
                    "property %s doesn't exist", key));
        }
    }

    protected String createProfilePropertyKey(String key, String profile) {
        return "migration.ds." + profile + "." + key;
    }
}
