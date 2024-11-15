/*
 *  Copyright: 2023 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.anonymizer;

import java.util.List;
import java.util.stream.Collectors;

public class TextEvaluator {
    private final AnonymizeFunctions anonymizeFunctions;

    public TextEvaluator() {
        this.anonymizeFunctions = new AnonymizeFunctions();
    }

    public TextEvaluator(AnonymizeFunctions anonymizeFunctions) {
        this.anonymizeFunctions = anonymizeFunctions;
    }

    public String getForTokens(final List<String> tokens) {
        return tokens.stream().map(token -> isTokenFunction(token) ? anonymizeFunctions.getForFunction(token) : token)
                .collect(Collectors.joining());
    }

    private boolean isTokenFunction(String token) {
        return token.startsWith("{") && token.endsWith("}");
    }
}
