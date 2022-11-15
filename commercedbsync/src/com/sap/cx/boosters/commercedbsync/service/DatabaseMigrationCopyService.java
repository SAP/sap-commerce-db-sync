/*
 *  Copyright: 2022 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */
package com.sap.cx.boosters.commercedbsync.service;

import com.sap.cx.boosters.commercedbsync.context.CopyContext;


/**
 * Actual Service to perform the Migration
 */
public interface DatabaseMigrationCopyService {

    void copyAllAsync(CopyContext context);

}
