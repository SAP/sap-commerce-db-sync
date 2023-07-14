/*
 *  Copyright: 2023 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.dataset.impl;

import com.sap.cx.boosters.commercedbsync.dataset.DataColumn;

public class DefaultDataColumn implements DataColumn {

    private final String name;
    private final int type;
    private final int precision;
    private final int scale;

    public DefaultDataColumn(String name, int type, int precision, int scale) {
        this.name = name;
        this.type = type;
        this.precision = precision;
        this.scale = scale;
    }

    @Override
    public String getColumnName() {
        return name;
    }

    @Override
    public int getColumnType() {
        return type;
    }

    @Override
    public int getPrecision() {
        return precision;
    }

    @Override
    public int getScale() {
        return scale;
    }
}
