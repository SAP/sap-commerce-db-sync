/*
 *  Copyright: 2023 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.context.validation;

import com.sap.cx.boosters.commercedbsync.context.MigrationContext;

public interface MigrationContextValidator {

    void validateContext(MigrationContext context);

}
