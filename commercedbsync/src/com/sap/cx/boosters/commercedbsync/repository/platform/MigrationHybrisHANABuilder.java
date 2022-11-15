/*
 *  Copyright: 2022 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.repository.platform;

import de.hybris.bootstrap.ddl.DatabaseSettings;
import de.hybris.bootstrap.ddl.sql.ColumnNativeTypeDecorator;
import de.hybris.bootstrap.ddl.sql.HanaSqlBuilder;
import de.hybris.bootstrap.ddl.sql.HybrisMSSqlBuilder;
import org.apache.ddlutils.Platform;
import org.apache.ddlutils.model.Column;

import java.sql.Types;

public class MigrationHybrisHANABuilder extends HanaSqlBuilder {

    public MigrationHybrisHANABuilder(Platform platform, DatabaseSettings databaseSettings,
                                      final Iterable<ColumnNativeTypeDecorator> columnNativeTypeDecorators) {
        super(platform, databaseSettings,columnNativeTypeDecorators);
    }

    @Override
    protected String getSqlType(Column column) {
        /*
           core-advanced-deployment.xml:661
           TODO implement more generic mapper for special attrs
         */
        final String nativeType = this.getNativeType(column);

        final int sizePos = nativeType.indexOf(SIZE_PLACEHOLDER);
        final StringBuilder sqlType = new StringBuilder();

          if((column.getTypeCode() == Types.NVARCHAR) && Integer.parseInt(column.getSize()) > 5000){
            return sqlType.append("NCLOB").toString();
            }

        sqlType.append(sizePos >= 0 ? nativeType.substring(0, sizePos) : nativeType);

        Object sizeSpec = column.getSize();
        if (sizeSpec == null) {
            sizeSpec = this.getPlatformInfo().getDefaultSize(column.getTypeCode());
        }

        if (sizeSpec != null)
        {
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

    //ddlutils cannot handle "complex" sizes ootb, therefore adding support here
    private String detectSize(Column column) {
        if (this.getPlatformInfo().hasSize(column.getTypeCode())) {
            if (column.getTypeCode() == Types.NVARCHAR) {
                if (column.getSizeAsInt() > 255 && column.getSizeAsInt() <=5000 ) {
                    return ""+ 5000;
                }
            } else if (column.getTypeCode() == Types.DOUBLE) {
                    return "30,8";
            }
        }
        return column.getSize();
    }
}
