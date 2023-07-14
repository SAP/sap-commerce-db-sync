/*
 *  Copyright: 2023 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.repository.platform;

import de.hybris.bootstrap.ddl.DatabaseSettings;
import de.hybris.bootstrap.ddl.HybrisPlatform;
import de.hybris.bootstrap.ddl.sql.HybrisMySqlBuilder;
import org.apache.ddlutils.PlatformInfo;
import org.apache.ddlutils.model.Column;
import org.apache.ddlutils.model.Table;
import org.apache.ddlutils.platform.mysql.MySql50ModelReader;
import org.apache.ddlutils.platform.mysql.MySql50Platform;

import java.util.Objects;

public class MigrationHybrisMySqlPlatform extends MySql50Platform implements HybrisPlatform {
    private static final String MYSQL_ALLOW_FRACTIONAL_SECONDS = "mysql.allow.fractional.seconds";
    private final boolean isFractionalSecondsSupportEnabled;

    private MigrationHybrisMySqlPlatform(boolean isFractionalSecondsSupportEnabled) {
        this.isFractionalSecondsSupportEnabled = isFractionalSecondsSupportEnabled;
    }

    public static HybrisPlatform build(DatabaseSettings databaseSettings) {
        Objects.requireNonNull(databaseSettings);
        boolean allowFractionaSeconds = Boolean
                .parseBoolean(databaseSettings.getProperty(MYSQL_ALLOW_FRACTIONAL_SECONDS, Boolean.TRUE.toString()));
        MigrationHybrisMySqlPlatform instance = new MigrationHybrisMySqlPlatform(allowFractionaSeconds);
        instance.provideCustomMapping();
        instance.setSqlBuilder(new MigrationHybrisMySqlBuilder(instance, databaseSettings));
        MySql50ModelReader reader = new MySql50ModelReader(instance);
        reader.setDefaultTablePattern(databaseSettings.getTablePrefix() + "%");
        instance.setModelReader(reader);
        return instance;
    }

    private void provideCustomMapping() {
        PlatformInfo platformInfo = this.getPlatformInfo();
        platformInfo.setMaxColumnNameLength(30);
        platformInfo.addNativeTypeMapping(-1, "TEXT");
        platformInfo.addNativeTypeMapping(12002, "BIGINT", -5);
        platformInfo.addNativeTypeMapping(12000, "TEXT", -1);
        platformInfo.addNativeTypeMapping(12003, "LONGTEXT", -1);
        platformInfo.addNativeTypeMapping(12001, "TEXT", -1);
        platformInfo.addNativeTypeMapping(12, "VARCHAR", 12);
        platformInfo.setDefaultSize(12, 255);
        platformInfo.addNativeTypeMapping(6, "FLOAT{0}");
        platformInfo.setHasPrecisionAndScale(6, true);

        platformInfo.addNativeTypeMapping(-5, "BIGINT");
        platformInfo.addNativeTypeMapping(-7, "TINYINT");
        platformInfo.addNativeTypeMapping(4, "INTEGER");
        platformInfo.addNativeTypeMapping(5, "INTEGER");
        platformInfo.addNativeTypeMapping(-6, "TINYINT", -6);
        platformInfo.addNativeTypeMapping(8, "FLOAT", 8);
        platformInfo.addNativeTypeMapping(-9, "VARCHAR", -9);
        platformInfo.setDefaultSize(-9, 255);
        platformInfo.setHasSize(-9, true);
        platformInfo.addNativeTypeMapping(92, "DATETIME", 93);
        platformInfo.addNativeTypeMapping(93, "DATETIME");
        platformInfo.addNativeTypeMapping(2004, "LONGBLOB");

        if (this.isFractionalSecondsSupportEnabled) {
            platformInfo.setHasSize(93, true);
            platformInfo.setDefaultSize(93, 6);
        }
    }

    public String getColumnName(Column column) {
        return ((HybrisMySqlBuilder) this.getSqlBuilder()).getColumnName(column);
    }

    public String getTableName(Table table) {
        return this.getSqlBuilder().getTableName(table);
    }
}
