/*
 *  Copyright: 2023 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.anonymizer.exception;

public class ValidationException extends RuntimeException {
    public ValidationException(String text) {
        super(text);
    }
}
