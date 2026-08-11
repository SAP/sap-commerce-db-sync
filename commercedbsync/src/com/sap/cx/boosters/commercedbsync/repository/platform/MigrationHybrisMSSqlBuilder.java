/*
 *  Copyright: 2026 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.repository.platform;

import de.hybris.bootstrap.ddl.DatabaseSettings;
import de.hybris.bootstrap.ddl.sql.HybrisMSSqlBuilder;
import org.apache.ddlutils.Platform;
import org.apache.ddlutils.model.Column;

import java.sql.Types;
import java.util.Set;

public class MigrationHybrisMSSqlBuilder extends HybrisMSSqlBuilder {

    public MigrationHybrisMSSqlBuilder(Platform platform, DatabaseSettings databaseSettings) {
        super(platform, databaseSettings);
    }

    @Override
    protected String getSqlType(Column column) {
        /*
         * core-advanced-deployment.xml:884 TODO implement more generic mapper for
         * special attrs
         */
        if ("InheritancePathString".equalsIgnoreCase(column.getName())) {
            return "NVARCHAR(4000)";
        }

        if (column.getTypeCode() == Types.NUMERIC) {
            if (column.getSizeAsInt() == 20 && column.getScale() == 0) {
                return "BIGINT";
            } else if (column.getSizeAsInt() == 1 && column.getScale() == 0) {
                return "TINYINT"; // no option here to check if primitive boolean, hence no default
            } else if (isKnownCharacterColumn(column)) {
                return "CHAR(4) DEFAULT ''";
            }
        }

        String nativeType = this.getNativeType(column);
        int sizePos = nativeType.indexOf("{0}");
        StringBuilder sqlType = new StringBuilder();
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

    private boolean isKnownCharacterColumn(Column column) {
        return CHAR_COLUMNS.contains(column.getName().toLowerCase());
    }

    private static final Set<String> CHAR_COLUMNS = Set.of("p_fieldseparator", "p_quotecharacter",
            "p_commentcharacter");
}
