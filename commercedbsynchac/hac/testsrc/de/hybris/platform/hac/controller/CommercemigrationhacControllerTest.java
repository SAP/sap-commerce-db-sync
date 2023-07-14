/*
 *  Copyright: 2023 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package de.hybris.platform.hac.controller;

import de.hybris.bootstrap.annotations.IntegrationTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Test for {@link CommercemigrationhacController}.
 */
@IntegrationTest
public class CommercemigrationhacControllerTest {

    /**
     * Code under test.
     */
    protected CommercemigrationhacController cut;

    /**
     * Set up the code under test.
     */
    @Before
    public void setup() {
        cut = new CommercemigrationhacController();
    }

    /**
     * Clean up the code under test.
     */
    @After
    public void teardown() {
        cut = null;
    }

    @Test
    public void testSayHello() {
        /*
         * final String helloText = cut.sayHello();
         *
         * assertNotNull(helloText); assertNotEquals(0, helloText.length());
         */
    }
}
