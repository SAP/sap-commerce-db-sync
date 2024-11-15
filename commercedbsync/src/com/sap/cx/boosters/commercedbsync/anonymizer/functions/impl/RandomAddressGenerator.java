/*
 *  Copyright: 2023 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.anonymizer.functions.impl;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.security.SecureRandom;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RandomAddressGenerator {
    private static final String CITY = "city";
    private static final String STREET = "street";

    private final JSONParser parser;
    private final Map<String, List<String>> map;
    private final SecureRandom random;
    private static final Logger LOGGER = LogManager.getLogger(RandomAddressGenerator.class);

    public RandomAddressGenerator() {
        this.parser = new JSONParser();
        this.map = getAddressMap();
        this.random = new SecureRandom();
    }

    public String generateRandomCity() {
        final List<String> city = map.get(CITY);
        return city.get(random.nextInt(city.size()));
    }

    public String generateRandomStreet() {
        final List<String> streets = map.get(STREET);
        return streets.get(random.nextInt(streets.size()));
    }

    @SuppressWarnings("unchecked")
    private Map<String, List<String>> getAddressMap() {
        Map<String, List<String>> addressMap = new HashMap<>();
        try (final InputStream inputStream = RandomAddressGenerator.class.getClassLoader()
                .getResourceAsStream("./anonymizer/address.json")) {
            if (inputStream == null) {
                return Collections.emptyMap();
            }
            JSONObject jsonObject = (JSONObject) parser.parse(new InputStreamReader(inputStream));
            List<String> cityList = (List<String>) jsonObject.get(CITY);
            List<String> streetList = (List<String>) jsonObject.get(STREET);
            addressMap.put(CITY, cityList);
            addressMap.put(STREET, streetList);
        } catch (IOException e) {
            LOGGER.error("IOException: {}", e.getMessage());
        } catch (ParseException e) {
            LOGGER.error("ParseException: {} ", e.getMessage());
        }
        return addressMap;
    }
}
