/*
 *  Copyright: 2023 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.processors.impl;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.ClassPathResource;

import com.sap.cx.boosters.commercedbsync.context.CopyContext;
import com.sap.cx.boosters.commercedbsync.context.MigrationContext;
import com.sap.cx.boosters.commercedbsync.processors.MigrationPreProcessor;
import com.sap.cx.boosters.commercedbsync.repository.DataRepository;

import de.hybris.bootstrap.ddl.DataBaseProvider;

public class TransformFunctionGeneratorPreProcessor implements MigrationPreProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(TransformFunctionGeneratorPreProcessor.class);

    @Override
    public void process(final CopyContext context) {
        final MigrationContext migrationContext = context.getMigrationContext();
        final DataRepository dataSourceRepository = migrationContext.getDataSourceRepository();
        final String platformSpecificSQL = getPlatformSpecificSQL(dataSourceRepository.getDatabaseProvider());
        if (StringUtils.isNotBlank(platformSpecificSQL)) {
            dataSourceRepository.runSqlScriptOnPrimary(
                    new ClassPathResource("/sql/transformationFunctions/" + platformSpecificSQL));
        }
    }

    private String getPlatformSpecificSQL(final DataBaseProvider databaseProvider) {
        String platformSpecificSQL = null;

        if (databaseProvider.isMssqlUsed()) {
            platformSpecificSQL = "mssql-general.sql";
        }

        LOG.info("Identified platform specific transformation function SQL: {}",
                StringUtils.defaultIfEmpty(platformSpecificSQL, "<none>"));

        return platformSpecificSQL;
    }

    @Override
    public boolean shouldExecute(CopyContext context) {
        return context.getMigrationContext().isDataSynchronizationEnabled()
                && context.getMigrationContext().getDataSourceRepository().getDatabaseProvider().isMssqlUsed();
    }
}
