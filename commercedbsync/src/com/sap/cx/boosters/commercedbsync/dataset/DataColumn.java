/*
 *  Copyright: 2022 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.dataset;

public interface DataColumn {

    String getColumnName();

    int getColumnType();

    int getPrecision();

    int getScale();

}
