/*
 *  Copyright: 2023 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.anonymizer.functions;

public interface FunctionEvaluator {
    boolean canApply(String function);
    String getForFunction(String function);
}
