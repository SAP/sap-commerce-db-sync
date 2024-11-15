/*
 *  Copyright: 2023 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.anonymizer;

import com.sap.cx.boosters.commercedbsync.anonymizer.functions.FunctionEvaluator;
import com.sap.cx.boosters.commercedbsync.anonymizer.functions.impl.GuidFunctionEvaluator;
import com.sap.cx.boosters.commercedbsync.anonymizer.functions.impl.RandomFunctionEvaluator;

import java.util.List;

public class AnonymizeFunctions {
    private static final List<FunctionEvaluator> evaluators = List.of(new GuidFunctionEvaluator(),
            new RandomFunctionEvaluator());

    public String getForFunction(final String function) {
        final String thisFunction = function.replaceAll("[{}]", "").trim();
        return evaluators.stream().filter(evaluator1 -> evaluator1.canApply(thisFunction)).findAny()
                .map(evaluator -> evaluator.getForFunction(thisFunction)).orElse("");
    }
}
