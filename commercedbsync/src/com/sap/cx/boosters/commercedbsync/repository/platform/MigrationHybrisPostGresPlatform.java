/*
 *  Copyright: 2022 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.repository.platform;

import de.hybris.bootstrap.ddl.DatabaseSettings;
import de.hybris.bootstrap.ddl.HybrisPlatform;
import org.apache.ddlutils.PlatformInfo;
import org.apache.ddlutils.model.Column;
import org.apache.ddlutils.model.Table;
import org.apache.ddlutils.platform.postgresql.PostgreSqlPlatform;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.sql.Types;

public class MigrationHybrisPostGresPlatform extends PostgreSqlPlatform implements HybrisPlatform {

    private static final Logger LOG = LoggerFactory.getLogger(MigrationHybrisPostGresPlatform.class);


    private MigrationHybrisPostGresPlatform() {
        super();
    }

    public static HybrisPlatform build(DatabaseSettings databaseSettings) {
        MigrationHybrisPostGresPlatform instance = new MigrationHybrisPostGresPlatform();
        instance.provideCustomMapping();
        instance.setSqlBuilder(new MigrationHybrisPostGresBuilder(instance));
        return instance;
    }


    private void provideCustomMapping() {
        PlatformInfo platformInfo = this.getPlatformInfo();
        platformInfo.setMaxColumnNameLength(31);
        platformInfo.addNativeTypeMapping(Types.NVARCHAR, "VARCHAR", Types.VARCHAR);
         platformInfo.addNativeTypeMapping(Types.NCHAR, "int2", Types.TINYINT);
         platformInfo.addNativeTypeMapping(Types.CHAR, "int2", Types.TINYINT);
        platformInfo.setHasSize(Types.CHAR, false);
        platformInfo.setHasSize(Types.NCHAR, false);
        platformInfo.setHasSize(Types.NVARCHAR, true);
        platformInfo.setHasSize(Types.VARCHAR, true);
        platformInfo.addNativeTypeMapping(Types.BIGINT, "int8");
        platformInfo.addNativeTypeMapping(Types.INTEGER, "int2");
        platformInfo.addNativeTypeMapping(Types.SMALLINT, "int2");
        platformInfo.addNativeTypeMapping(Types.TINYINT, "int2");
        platformInfo.addNativeTypeMapping(Types.DOUBLE, "float8");
    }

    @Override
    public String getTableName(Table table) {
        return this.getSqlBuilder().getTableName(table);
    }

    public String getColumnName(Column column) {
        return ((MigrationHybrisPostGresBuilder) this.getSqlBuilder()).getColumnName(column);
    }
}
