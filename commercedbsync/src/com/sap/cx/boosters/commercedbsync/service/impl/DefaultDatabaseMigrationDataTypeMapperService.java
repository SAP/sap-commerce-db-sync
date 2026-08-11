/*
 *  Copyright: 2026 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.service.impl;

import com.google.common.io.ByteStreams;
import com.sap.cx.boosters.commercedbsync.context.MigrationContext;
import org.apache.commons.io.IOUtils;
import com.sap.cx.boosters.commercedbsync.service.DatabaseMigrationDataTypeMapperService;
import org.apache.commons.lang3.StringUtils;
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
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.TimeZone;

/**
 *
 */
public class DefaultDatabaseMigrationDataTypeMapperService implements DatabaseMigrationDataTypeMapperService {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultDatabaseMigrationDataTypeMapperService.class);

    private boolean shiftTimeZone;
    private ZoneId sourceTimeZone;

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
        } else if (jdbcType == Types.TIMESTAMP) {
            if (shiftTimeZone && !ZoneOffset.UTC.equals(sourceTimeZone)
                    && targetColumnValue instanceof LocalDateTime dateTime) {
                ZonedDateTime zonedDateTime = dateTime.atZone(sourceTimeZone);
                ZonedDateTime utcTime = zonedDateTime.withZoneSameInstant(ZoneOffset.UTC);
                targetColumnValue = utcTime.toLocalDateTime();
            }
        }

        return targetColumnValue;
    }

    @Override
    public void beforeMigration(MigrationContext context) {
        sourceTimeZone = ZoneOffset.UTC;
        shiftTimeZone = false;

        if (!context.isAdjustTimestampsToUTC()) {
            return;
        }

        if (context.isDataSynchronizationEnabled()) {
            LOG.debug("Time zone check will not be performed in synchronization mode");
            return;
        }

        String databaseTimezone = context.getDataSourceRepository().getDatabaseTimezone();

        if (StringUtils.isEmpty(databaseTimezone)) {
            LOG.warn("Unable to get source time zone");
        } else {
            try {
                TimeZone source = TimeZone.getTimeZone(databaseTimezone);

                if (TimeZone.getTimeZone("UTC").getRawOffset() != source.getRawOffset()) {
                    sourceTimeZone = source.toZoneId();
                    shiftTimeZone = true;
                    LOG.info("The time zone on source is not UTC, timestamp values will be adjusted");
                }
            } catch (Exception e) {
                LOG.warn("Unable to parse source time zone", e);
            }
        }
    }

    private String getValue(final NClob nClob) throws SQLException, IOException {
        return getValue(nClob.getCharacterStream());
    }

    private String getValue(final Clob clob) throws SQLException, IOException {
        return getValue(clob.getCharacterStream());
    }

    private String getValue(final Reader in) throws IOException {
        final StringWriter w = new StringWriter();
        IOUtils.copy(in, w);
        String value = w.toString();
        w.close();
        in.close();
        return value;
    }
}
