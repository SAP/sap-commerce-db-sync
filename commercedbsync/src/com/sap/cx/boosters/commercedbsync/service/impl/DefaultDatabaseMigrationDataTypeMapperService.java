/*
 *  Copyright: 2023 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.service.impl;

import com.google.common.io.ByteStreams;
import org.apache.commons.io.IOUtils;
import com.sap.cx.boosters.commercedbsync.service.DatabaseMigrationDataTypeMapperService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.Reader;
import java.io.StringWriter;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.NClob;
import java.sql.SQLException;
import java.sql.Types;

/**
 *
 */
public class DefaultDatabaseMigrationDataTypeMapperService implements DatabaseMigrationDataTypeMapperService {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultDatabaseMigrationDataTypeMapperService.class);

    @Override
    public Object dataTypeMapper(final Object sourceColumnValue, final int jdbcType) throws IOException, SQLException {
        Object targetColumnValue = sourceColumnValue;
        if (sourceColumnValue == null) {
            // do nothing
        } else if (jdbcType == Types.BLOB) {
            targetColumnValue = new ByteArrayInputStream(
                    ByteStreams.toByteArray(((Blob) sourceColumnValue).getBinaryStream()));
        } else if (jdbcType == Types.NCLOB) {
            targetColumnValue = getValue((NClob) sourceColumnValue);
        } else if (jdbcType == Types.CLOB) {
            targetColumnValue = getValue((Clob) sourceColumnValue);
        }
        return targetColumnValue;
    }

    private String getValue(final NClob nClob) throws SQLException, IOException {
        return getValue(nClob.getCharacterStream());
    }

    private String getValue(final Clob clob) throws SQLException, IOException {
        return getValue(clob.getCharacterStream());
    }

    private String getValue(final Reader in) throws SQLException, IOException {
        final StringWriter w = new StringWriter();
        IOUtils.copy(in, w);
        String value = w.toString();
        w.close();
        in.close();
        return value;
    }

}
