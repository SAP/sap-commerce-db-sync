/*
 *  Copyright: 2023 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.processors.impl;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.sap.cx.boosters.commercedbsync.context.CopyContext;
import com.sap.cx.boosters.commercedbsync.service.DatabaseMigrationReportService;
import com.sap.cx.boosters.commercedbsync.service.DatabaseMigrationReportStorageService;
import com.sap.cx.boosters.commercedbsync.utils.LocalDateTypeAdapter;
import com.sap.cx.boosters.commercedbsync.MigrationReport;
import com.sap.cx.boosters.commercedbsync.processors.MigrationPostProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;

public class ReportMigrationPostProcessor implements MigrationPostProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(ReportMigrationPostProcessor.class.getName());

    private DatabaseMigrationReportService databaseMigrationReportService;
    private DatabaseMigrationReportStorageService databaseMigrationReportStorageService;

    @Override
    public void process(CopyContext context) {
        if (!databaseMigrationReportStorageService.validateConnection()) {
            LOG.warn("Could not establish connection to report storage. Migration report will not be stored");
            return;
        }

        try {
            final GsonBuilder gsonBuilder = new GsonBuilder();
            gsonBuilder.setPrettyPrinting();
            gsonBuilder.registerTypeAdapter(LocalDateTime.class, new LocalDateTypeAdapter());
            gsonBuilder.disableHtmlEscaping();
            Gson gson = gsonBuilder.create();
            MigrationReport migrationReport = databaseMigrationReportService.getMigrationReport(context);
            InputStream is = new ByteArrayInputStream(gson.toJson(migrationReport).getBytes(StandardCharsets.UTF_8));
            databaseMigrationReportStorageService.store(context.getMigrationId() + ".json", is);
            LOG.info("Finished writing database migration report");
        } catch (Exception e) {
            LOG.error("Error executing post processor", e);
        }
    }

    public void setDatabaseMigrationReportService(DatabaseMigrationReportService databaseMigrationReportService) {
        this.databaseMigrationReportService = databaseMigrationReportService;
    }

    public void setDatabaseMigrationReportStorageService(
            DatabaseMigrationReportStorageService databaseMigrationReportStorageService) {
        this.databaseMigrationReportStorageService = databaseMigrationReportStorageService;
    }
}
