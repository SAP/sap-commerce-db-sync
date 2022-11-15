/*
 *  Copyright: 2022 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.utils;

public class MaskUtil {

    public static String stripJdbcPassword(final String jdbcConnectionString) {
        return jdbcConnectionString.replaceFirst("password=.*?;", "password=***;");
    }

}
