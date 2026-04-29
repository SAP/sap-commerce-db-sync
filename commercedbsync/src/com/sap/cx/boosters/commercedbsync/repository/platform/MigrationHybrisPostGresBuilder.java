/*
 *  Copyright: 2026 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.repository.platform;

import de.hybris.bootstrap.ddl.DatabaseSettings;
import de.hybris.bootstrap.ddl.sql.HybrisPostgreSqlBuilder;
import org.apache.commons.lang3.StringUtils;
import org.apache.ddlutils.Platform;
import org.apache.ddlutils.model.Column;
import org.apache.ddlutils.model.TypeMap;

import java.sql.Types;

public class MigrationHybrisPostGresBuilder extends HybrisPostgreSqlBuilder {

    public MigrationHybrisPostGresBuilder(Platform platform, DatabaseSettings databaseSettings) {
        super(platform, databaseSettings);
    }

    @Override
    protected String getSqlType(Column column) {

        String nativeType = this.getNativeType(column);
        int sizePos = nativeType.indexOf("{0}");
        StringBuilder sqlType = new StringBuilder();

        if ((column.getTypeCode() == Types.NVARCHAR) && Integer.parseInt(column.getSize()) > 5000) {
            return sqlType.append("text").toString();
        }

        sqlType.append(sizePos >= 0 ? nativeType.substring(0, sizePos) : nativeType);
        Object sizeSpec = column.getSize();
        if (sizeSpec == null) {
            sizeSpec = this.getPlatformInfo().getDefaultSize(column.getTypeCode());
        }

        if (sizeSpec != null) {
            if (this.getPlatformInfo().hasSize(column.getTypeCode())) {
                sqlType.append("(");
                sqlType.append(sizeSpec);
                sqlType.append(")");
            } else if (this.getPlatformInfo().hasPrecisionAndScale(column.getTypeCode())) {
                sqlType.append("(");
                sqlType.append(column.getSizeAsInt());
                sqlType.append(",");
                sqlType.append(column.getScale());
                sqlType.append(")");
            }
        }

        sqlType.append(sizePos >= 0 ? nativeType.substring(sizePos + "{0}".length()) : "");
        return sqlType.toString();
    }

    @Override
    public boolean isValidDefaultValue(String defaultSpec, int typeCode) {
        return defaultSpec != null && StringUtils.isNumeric(defaultSpec)
                && (defaultSpec.length() > 0 || !TypeMap.isNumericType(typeCode) && !TypeMap.isDateTimeType(typeCode));
    }

    @Override
    public String getColumnName(final Column column) {
        return column.getName();
    }
}
