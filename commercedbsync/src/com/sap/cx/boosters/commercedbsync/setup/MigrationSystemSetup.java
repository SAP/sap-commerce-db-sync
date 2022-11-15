/*
 *  Copyright: 2022 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.setup;

import com.sap.cx.boosters.commercedbsync.constants.CommercedbsyncConstants;
import de.hybris.platform.core.initialization.SystemSetup;
import de.hybris.platform.core.initialization.SystemSetupContext;
import de.hybris.platform.servicelayer.config.ConfigurationService;
import org.apache.commons.lang.StringUtils;
import com.sap.cx.boosters.commercedbsync.context.MigrationContext;
import com.sap.cx.boosters.commercedbsync.service.DatabaseMigrationSynonymService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class provides hooks into the system's initialization and update processes.
 */
@SystemSetup(extension = CommercedbsyncConstants.EXTENSIONNAME)
public class MigrationSystemSetup {

    private static final Logger LOG = LoggerFactory.getLogger(MigrationSystemSetup.class);

    private MigrationContext migrationContext;
    private ConfigurationService configurationService;
    private DatabaseMigrationSynonymService databaseMigrationSynonymService;

    public MigrationSystemSetup(MigrationContext migrationContext, ConfigurationService configurationService, DatabaseMigrationSynonymService databaseMigrationSynonymService) {
        this.migrationContext = migrationContext;
        this.configurationService = configurationService;
        this.databaseMigrationSynonymService = databaseMigrationSynonymService;
    }

    /**
     * CCv2 Workaround: ccv2 builder does not support prefixes yet.
     * creating synonym on ydeployments -> prefix_yeployments
     * creating synonym on attributedescriptors -> prefix_attributedescriptors.
     *
     * @param context
     * @throws Exception
     */
    @SystemSetup(type = SystemSetup.Type.ESSENTIAL, process = SystemSetup.Process.ALL)
    public void createEssentialData(final SystemSetupContext context) throws Exception {
        String actualPrefix = configurationService.getConfiguration().getString("db.tableprefix");
        if (StringUtils.isNotEmpty(actualPrefix)) {
            databaseMigrationSynonymService.recreateSynonyms(migrationContext.getDataTargetRepository(), actualPrefix);
        }
    }

}
