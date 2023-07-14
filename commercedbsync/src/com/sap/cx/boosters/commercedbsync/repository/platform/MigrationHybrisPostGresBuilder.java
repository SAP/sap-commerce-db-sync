/*
 *  Copyright: 2023 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.repository.platform;

import org.apache.commons.lang3.StringUtils;
import org.apache.ddlutils.Platform;
import org.apache.ddlutils.model.Column;
import org.apache.ddlutils.model.TypeMap;
import org.apache.ddlutils.platform.postgresql.PostgreSqlBuilder;

import java.sql.Types;

public class MigrationHybrisPostGresBuilder extends PostgreSqlBuilder {

    public MigrationHybrisPostGresBuilder(Platform platform) {
        super(platform);
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
                sqlType.append(detectSize(column));
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

    // ddlutils cannot handle "complex" sizes ootb, therefore adding support here
    private String detectSize(Column column) {
        if (this.getPlatformInfo().hasSize(column.getTypeCode())) {
            if (column.getTypeCode() == Types.NVARCHAR) {
                if (column.getSizeAsInt() > 4000) {
                    return "MAX";
                }
            }
            if (column.getTypeCode() == Types.VARCHAR) {
                if (column.getSizeAsInt() > 8000) {
                    return "MAX";
                }
            }
            if (column.getTypeCode() == Types.VARBINARY) {
                if (column.getSizeAsInt() > 8000) {
                    return "MAX";
                }
            }
        }
        return column.getSize();
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
