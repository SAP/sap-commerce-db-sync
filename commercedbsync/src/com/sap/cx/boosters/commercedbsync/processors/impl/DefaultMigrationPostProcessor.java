/*
 *  Copyright: 2022 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.processors.impl;

import com.sap.cx.boosters.commercedbsync.context.CopyContext;
import com.sap.cx.boosters.commercedbsync.processors.MigrationPostProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implements the {@link MigrationPostProcessor}
 */
public class DefaultMigrationPostProcessor implements MigrationPostProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultMigrationPostProcessor.class.getName());

    @Override
    public void process(CopyContext context) {
        LOG.info("DefaultMigrationPostProcessor Finished");
    }
}
