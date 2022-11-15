/*
 *  Copyright: 2022 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.service.impl;

import com.sap.cx.boosters.commercedbsync.constants.CommercedbsyncConstants;
import com.sap.cx.boosters.commercedbsync.repository.DataRepository;
import com.sap.cx.boosters.commercedbsync.service.DatabaseMigrationSynonymService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultDatabaseMigrationSynonymService implements DatabaseMigrationSynonymService {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultDatabaseMigrationSynonymService.class);

    private static final String YDEPLOYMENTS = CommercedbsyncConstants.DEPLOYMENTS_TABLE;
    private static final String ATTRDESCRIPTORS = "attributedescriptors";


    @Override
    public void recreateSynonyms(DataRepository repository, String prefix) throws Exception {
        recreateSynonym(repository, YDEPLOYMENTS, prefix);
        recreateSynonym(repository, ATTRDESCRIPTORS, prefix);
    }

    private void recreateSynonym(DataRepository repository, String table, String actualPrefix) throws Exception {
        LOG.info("Creating Synonym for {} on {}{}", table, actualPrefix, table);
        repository.executeUpdateAndCommit(String.format("DROP SYNONYM IF EXISTS %s", table));
        repository.executeUpdateAndCommit(String.format("CREATE SYNONYM %s FOR %s%s", table, actualPrefix, table));
    }
}
