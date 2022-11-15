/*
 *  Copyright: 2022 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.repository.platform;

import com.google.common.collect.ImmutableList;
import de.hybris.bootstrap.ddl.DatabaseSettings;
import de.hybris.bootstrap.ddl.HybrisHanaPlatform;
import de.hybris.bootstrap.ddl.HybrisPlatform;
import de.hybris.bootstrap.ddl.jdbc.PlatformJDBCMappingProvider;
import de.hybris.bootstrap.ddl.sql.ColumnNativeTypeDecorator;
import de.hybris.bootstrap.ddl.sql.HanaBlobColumnNativeTypeDecorator;
import org.apache.ddlutils.PlatformInfo;
import org.apache.ddlutils.model.JdbcTypeCategoryEnum;
import org.apache.ddlutils.model.TypeMap;
import org.apache.ddlutils.platform.SqlBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Types;

public class MigrationHybrisHANAPlatform extends HybrisHanaPlatform implements HybrisPlatform {

    private static final Logger LOG = LoggerFactory.getLogger(MigrationHybrisHANAPlatform.class);

    private SqlBuilder sqlBuilder;


    private MigrationHybrisHANAPlatform(final DatabaseSettings databaseSettings) {
        super(databaseSettings);
    }

    public static HybrisPlatform build(DatabaseSettings databaseSettings) {

        final MigrationHybrisHANAPlatform instance = new MigrationHybrisHANAPlatform(databaseSettings);
        HANAHybrisTypeMap.register();
        instance.provideCustomMapping();
        instance.setSqlBuilder(new MigrationHybrisHANABuilder(instance, databaseSettings, getNativeTypeDecorators(databaseSettings)));
        return instance;
    }
    private void provideCustomMapping()
    {
        final PlatformInfo platformInfo = getPlatformInfo();

        platformInfo.setMaxColumnNameLength(PlatformJDBCMappingProvider.MAX_COLUMN_NAME_LENGTH);

        platformInfo.addNativeTypeMapping(PlatformJDBCMappingProvider.HYBRIS_PK, "BIGINT", Types.BIGINT);
        platformInfo.addNativeTypeMapping(PlatformJDBCMappingProvider.HYBRIS_LONG_STRING, "NCLOB", Types.NCLOB);
        platformInfo.addNativeTypeMapping(PlatformJDBCMappingProvider.HYBRIS_JSON, "NCLOB", Types.LONGVARCHAR);
        platformInfo.addNativeTypeMapping(PlatformJDBCMappingProvider.HYBRIS_COMMA_SEPARATED_PKS, "NVARCHAR{0}", Types.NVARCHAR);
       // platformInfo.addNativeTypeMapping(2011, "NCLOB");

        platformInfo.setHasSize(PlatformJDBCMappingProvider.HYBRIS_LONG_STRING, true);
        platformInfo.setHasSize(PlatformJDBCMappingProvider.HYBRIS_COMMA_SEPARATED_PKS, true);

        platformInfo.addNativeTypeMapping(Types.BIT, "DECIMAL(1,0)", Types.NUMERIC);

        platformInfo.addNativeTypeMapping(Types.DECIMAL, "DECIMAL", Types.DECIMAL);
        platformInfo.setHasSize(Types.FLOAT, true);
        platformInfo.setHasSize(Types.DOUBLE, true);
        platformInfo.setHasSize(Types.NVARCHAR, true);
    }

    @Override
    public SqlBuilder getSqlBuilder() {
        return this.sqlBuilder;
    }

    @Override
    protected void setSqlBuilder(SqlBuilder builder) {
        this.sqlBuilder = builder;
    }

    private static Iterable<ColumnNativeTypeDecorator> getNativeTypeDecorators(final DatabaseSettings databaseSettings)
    {
        return ImmutableList.of(new HanaBlobColumnNativeTypeDecorator(databaseSettings));
    }
    static class HANAHybrisTypeMap extends TypeMap {

        static void register() {
            registerJdbcType(Types.NCHAR, "NVARCHAR", JdbcTypeCategoryEnum.TEXTUAL);
            registerJdbcType(Types.NCLOB, "NCLOB", JdbcTypeCategoryEnum.TEXTUAL);
        }
    }

}
