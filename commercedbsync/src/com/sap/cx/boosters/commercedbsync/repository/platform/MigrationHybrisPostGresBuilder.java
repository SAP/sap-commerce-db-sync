/*
 *  Copyright: 2026 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.repository.platform;

import de.hybris.bootstrap.ddl.DDLGeneratorUtils;
import de.hybris.bootstrap.ddl.DatabaseSettings;
import de.hybris.bootstrap.ddl.sql.HybrisPostgreSqlBuilder;
import org.apache.commons.lang3.StringUtils;
import org.apache.ddlutils.Platform;
import org.apache.ddlutils.alteration.ColumnChange;
import org.apache.ddlutils.model.Column;
import org.apache.ddlutils.model.Database;
import org.apache.ddlutils.model.Index;
import org.apache.ddlutils.model.Table;
import org.apache.ddlutils.model.TypeMap;

import java.io.IOException;
import java.sql.Types;
import java.util.List;
import java.util.Map;

public class MigrationHybrisPostGresBuilder extends HybrisPostgreSqlBuilder {
    private final DatabaseSettings databaseSettings;

    public MigrationHybrisPostGresBuilder(Platform platform, DatabaseSettings databaseSettings) {
        super(platform, databaseSettings);
        this.databaseSettings = databaseSettings;
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

    @Override
    public String getIndexName(Index index) {
        return DDLGeneratorUtils.getIndexName(index, this, this.databaseSettings, true);
    }

    @Override
    public void processTableStructureChanges(Database currentModel, Database desiredModel, Table sourceTable,
            Table targetTable, Map parameters, List changes) throws IOException {
        super.processTableStructureChanges(currentModel, desiredModel, sourceTable, targetTable, parameters, changes);

        for (Object change : changes) {
            if (change instanceof ColumnChange columnChange) {
                Column sourceColumn = columnChange.getChangedColumn();
                Column targetColumn = targetTable.findColumn(sourceColumn.getName(),
                        this.getPlatform().isDelimitedIdentifierModeOn());
                this.processColumnChange(sourceTable, targetTable, targetColumn);
            }
        }
    }

    protected void processColumnChange(Table sourceTable, Table targetTable, Column targetColumn) throws IOException {
        this.print("ALTER TABLE ");
        this.printlnIdentifier(this.getTableName(sourceTable));
        this.printIndent();
        this.print("ALTER COLUMN ");
        this.printIdentifier(this.getColumnName(targetColumn));
        this.print(" TYPE ");
        this.print(this.getSqlType(targetColumn));
        this.printEndOfStatement();
    }

}
