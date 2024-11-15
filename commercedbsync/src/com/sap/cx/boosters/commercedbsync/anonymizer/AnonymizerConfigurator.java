/*
 *  Copyright: 2023 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.anonymizer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.sap.cx.boosters.commercedbsync.anonymizer.model.AnonymizerConfiguration;
import org.springframework.core.io.ClassPathResource;

import java.io.IOException;

public class AnonymizerConfigurator {
    private final AnonymizerConfiguration anonymizerConfiguration;

    public AnonymizerConfigurator() {
        final ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory());
        try {
            anonymizerConfiguration = objectMapper.readValue(new ClassPathResource("./anonymizer/config.yml").getFile(),
                    AnonymizerConfiguration.class);
        } catch (IOException e) {
            throw new RuntimeException("Error reading anonymizer configuration", e);
        }
    }

    public AnonymizerConfiguration getConfiguration() {
        return anonymizerConfiguration;
    }
}
