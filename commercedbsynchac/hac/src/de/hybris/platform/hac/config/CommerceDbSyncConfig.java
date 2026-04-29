/*
 *  Copyright: 2026 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package de.hybris.platform.hac.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;

import java.util.Optional;

@Configuration
public class CommerceDbSyncConfig {
    @Bean
    public ObjectMapper objectMapper(@Autowired(required = false) MappingJackson2HttpMessageConverter converter) {
        ObjectMapper mapper = Optional.ofNullable(converter).map(MappingJackson2HttpMessageConverter::getObjectMapper)
                .orElseGet(ObjectMapper::new);
        mapper.registerModule(new JavaTimeModule());
        mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        return mapper;
    }
}
