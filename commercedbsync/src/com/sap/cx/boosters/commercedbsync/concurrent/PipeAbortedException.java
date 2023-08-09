/*
 *  Copyright: 2023 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.concurrent;

public class PipeAbortedException extends Exception {
    public PipeAbortedException(String message) {
        super(message);
    }

    public PipeAbortedException(String message, Throwable cause) {
        super(message, cause);
    }
}
