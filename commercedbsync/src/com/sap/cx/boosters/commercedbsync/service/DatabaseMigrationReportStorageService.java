/*
 *  Copyright: 2023 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.service;

import java.io.InputStream;

public interface DatabaseMigrationReportStorageService {
    void store(String fileName, InputStream inputStream) throws Exception;

    boolean validateConnection();
}
