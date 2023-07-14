/*
 *  Copyright: 2023 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.repository.platform;

import de.hybris.bootstrap.ddl.DatabaseSettings;
import de.hybris.bootstrap.ddl.sql.HybrisMySqlBuilder;
import org.apache.commons.lang3.StringUtils;
import org.apache.ddlutils.Platform;
import org.apache.ddlutils.model.Column;
import org.apache.ddlutils.model.TypeMap;

import java.sql.Types;

public class MigrationHybrisMySqlBuilder extends HybrisMySqlBuilder {

    public MigrationHybrisMySqlBuilder(Platform platform, DatabaseSettings databaseSettings) {
        super(platform, databaseSettings);
    }

    @Override
    protected String getSqlType(Column column) {
        if (column.getTypeCode() == Types.NVARCHAR && Integer.parseInt(column.getSize()) > 5000) {
            return "TEXT";
        }

        if (column.getTypeCode() == Types.VARBINARY && Integer.parseInt(column.getSize()) > 65535) {
            return "LONGBLOB";
        }

        if (column.getTypeCode() == Types.TIMESTAMP) {
            final StringBuilder nativeType = new StringBuilder(getPlatformInfo().getNativeType(column.getTypeCode()));

            if (getPlatformInfo().hasSize(column.getTypeCode())) {
                nativeType.append('(').append(getPlatformInfo().getDefaultSize(column.getTypeCode())).append(')');
            }

            return nativeType.toString();
        }

        return super.getSqlType(column);
    }

    @Override
    public boolean isValidDefaultValue(String defaultSpec, int typeCode) {
        return StringUtils.isNumeric(defaultSpec)
                && (defaultSpec.length() > 0 || !TypeMap.isNumericType(typeCode) && !TypeMap.isDateTimeType(typeCode));
    }
}
