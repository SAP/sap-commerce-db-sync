/*
 *  Copyright: 2023 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.anonymizer.model;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class Column {
    private String name;
    private Operation operation;
    private String text;
    private List<Object> exclude = new ArrayList<>();
    private List<Object> excludeRow = new ArrayList<>();

    public Column() {
    }

    public Column(final String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }
    public void setName(String name) {
        this.name = name;
    }
    public Operation getOperation() {
        return operation;
    }
    public void setOperation(Operation operation) {
        this.operation = operation;
    }

    public String getText() {
        return text;
    }
    public void setText(String text) {
        this.text = text;
    }

    public List<Object> getExclude() {
        return exclude;
    }
    public void setExclude(List<Object> exclude) {
        this.exclude = exclude;
    }

    public List<Object> getExcludeRow() {
        return excludeRow;
    }
    public void setExcludeRow(List<Object> excludeRow) {
        this.excludeRow = excludeRow;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Column column = (Column) o;
        return Objects.equals(name, column.name);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(name);
    }
}
