/*
 *  Copyright: 2023 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.service.impl;

import com.sap.cx.boosters.commercedbsync.constants.CommercedbsyncConstants;
import com.sap.cx.boosters.commercedbsync.scheduler.DatabaseCopyScheduler;
import com.sap.cx.boosters.commercedbsync.utils.MaskUtil;
import de.hybris.platform.servicelayer.config.ConfigurationService;
import org.apache.commons.configuration.Configuration;
import com.sap.cx.boosters.commercedbsync.MigrationReport;
import com.sap.cx.boosters.commercedbsync.context.CopyContext;
import com.sap.cx.boosters.commercedbsync.service.DatabaseCopyTaskRepository;
import com.sap.cx.boosters.commercedbsync.service.DatabaseMigrationReportService;

import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Collectors;

public class DefaultDatabaseMigrationReportService implements DatabaseMigrationReportService {

    private DatabaseCopyScheduler databaseCopyScheduler;
    private DatabaseCopyTaskRepository databaseCopyTaskRepository;
    private ConfigurationService configurationService;

    @Override
    public MigrationReport getMigrationReport(CopyContext copyContext) throws Exception {
        final MigrationReport migrationReport = new MigrationReport();
        migrationReport.setMigrationID(copyContext.getMigrationId());
        populateConfiguration(migrationReport);
        migrationReport.setMigrationStatus(databaseCopyScheduler.getCurrentState(copyContext, OffsetDateTime.MAX));
        migrationReport.setDatabaseCopyTasks(databaseCopyTaskRepository.getAllTasks(copyContext));
        return migrationReport;
    }

    private void populateConfiguration(MigrationReport migrationReport) {
        final SortedMap<String, String> configuration = new TreeMap<>();
        final Configuration config = configurationService.getConfiguration();
        final Configuration subset = config.subset(CommercedbsyncConstants.PROPERTIES_PREFIX);
        final Set<String> maskedProperties = Arrays
                .stream(config.getString(CommercedbsyncConstants.MIGRATION_REPORT_MASKED_PROPERTIES).split(","))
                .collect(Collectors.toSet());

        final Iterator<String> keys = subset.getKeys();

        while (keys.hasNext()) {
            final String key = keys.next();
            final String prefixedKey = CommercedbsyncConstants.PROPERTIES_PREFIX + "." + key;

            if (CommercedbsyncConstants.MIGRATION_REPORT_MASKED_PROPERTIES.equals(prefixedKey)) {
                continue;
            }

            configuration.put(prefixedKey,
                    maskedProperties.contains(prefixedKey)
                            ? CommercedbsyncConstants.MASKED_VALUE
                            : MaskUtil.stripJdbcPassword(subset.getString(key)));
        }

        migrationReport.setConfiguration(configuration);
    }

    public void setDatabaseCopyScheduler(DatabaseCopyScheduler databaseCopyScheduler) {
        this.databaseCopyScheduler = databaseCopyScheduler;
    }

    public void setDatabaseCopyTaskRepository(DatabaseCopyTaskRepository databaseCopyTaskRepository) {
        this.databaseCopyTaskRepository = databaseCopyTaskRepository;
    }

    public void setConfigurationService(ConfigurationService configurationService) {
        this.configurationService = configurationService;
    }
}
