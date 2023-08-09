/*
 *  Copyright: 2023 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.service;

import com.sap.cx.boosters.commercedbsync.MigrationReport;
import com.sap.cx.boosters.commercedbsync.context.CopyContext;

public interface DatabaseMigrationReportService {

    MigrationReport getMigrationReport(CopyContext copyContext) throws Exception;

}
