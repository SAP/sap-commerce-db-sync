/*
 *  Copyright: 2023 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.anonymizer.functions.impl;

import com.sap.cx.boosters.commercedbsync.anonymizer.functions.FunctionEvaluator;

import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RandomFunctionEvaluator implements FunctionEvaluator {
    private final RandomAddressGenerator randomAddressGenerator;
    private final Random random;
    private static final Pattern PATTERN = Pattern.compile("\\d+");

    public RandomFunctionEvaluator() {
        this.randomAddressGenerator = new RandomAddressGenerator();
        this.random = new Random();
    }

    @Override
    public boolean canApply(String function) {
        return function.startsWith("RANDOM");
    }

    @Override
    public String getForFunction(String function) {
        if (function.contains("city")) {
            return randomAddressGenerator.generateRandomCity();
        } else if (function.contains("street")) {
            return randomAddressGenerator.generateRandomStreet();
        }

        String length;
        Matcher matcher = PATTERN.matcher(function);
        if (matcher.find()) {
            length = matcher.group();
        } else {
            throw new IllegalArgumentException(
                    "Invalid function format. Expected RANDOM(type,length) - length must be a number");
        }

        try {
            int lengthValue = Integer.parseInt(length);
            if (function.contains("number")) {
                return generateRandomNumber(lengthValue);
            } else if (function.contains("string")) {
                return generateRandomString(lengthValue);
            } else {
                throw new IllegalArgumentException(
                        "Invalid value! Allowed values for function RANDOM(type,length) are: type=string or type=number");
            }
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid length value: " + length, e);
        }
    }

    private String generateRandomNumber(int length) {
        if (length < 2) {
            throw new IllegalArgumentException("Length must be at least 2");
        }

        StringBuilder result = new StringBuilder(length);

        result.append(random.nextInt(9) + 1);

        for (int i = 1; i < length; i++) {
            result.append(random.nextInt(10));
        }

        return result.toString();
    }

    private String generateRandomString(int length) {
        String upperCaseLetters = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
        String lowerCaseLetters = upperCaseLetters.toLowerCase();
        String allLetters = upperCaseLetters + lowerCaseLetters;
        if (length <= 0) {
            throw new IllegalArgumentException("Length must be greater than 0");
        }

        StringBuilder result = new StringBuilder(length);

        for (int i = 0; i < length; i++) {
            int randomIndex = random.nextInt(allLetters.length());
            result.append(allLetters.charAt(randomIndex));
        }

        return result.toString();
    }
}
