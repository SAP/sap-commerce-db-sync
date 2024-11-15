/*
 *  Copyright: 2023 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.anonymizer.functions.impl;

import com.sap.cx.boosters.commercedbsync.anonymizer.functions.FunctionEvaluator;

import java.util.UUID;

public class GuidFunctionEvaluator implements FunctionEvaluator {
    @Override
    public boolean canApply(String function) {
        return function.equals("GUID");
    }

    @Override
    public String getForFunction(String function) {
        return UUID.randomUUID().toString();
    }
}
