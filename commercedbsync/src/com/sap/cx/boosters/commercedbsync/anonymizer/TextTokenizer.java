/*
 *  Copyright: 2023 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.anonymizer;

import com.sap.cx.boosters.commercedbsync.anonymizer.exception.ValidationException;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TextTokenizer {
    private static final Pattern TOKEN_PATTERN = Pattern.compile("\\{[^}]*}|[^{}]+");

    public List<String> tokenizeText(final String text) {
        checkForErrors(text);
        if (StringUtils.isBlank(text)) {
            return List.of();
        }

        if (!text.contains("{")) {
            return List.of(text.trim());
        }

        final Matcher matcher = TOKEN_PATTERN.matcher(text);
        final List<String> tokens = new ArrayList<>();
        while (matcher.find()) {
            tokens.add(matcher.group());
        }
        return tokens;
    }

    private void checkForErrors(String text) {
        if (StringUtils.countMatches(text, "{") != StringUtils.countMatches(text, "}")
                || StringUtils.countMatches(text, "(") != StringUtils.countMatches(text, ")")) {
            throw new ValidationException(
                    "Text " + text + " has invalid defined functions (not closing/opening functions)");
        }
    }
}
