/*
 *  Copyright: 2023 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.context;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class LaunchOptions {

    public static final LaunchOptions NONE = new LaunchOptions();

    private final Map<String, Serializable> propertyOverrideMap;

    public LaunchOptions() {
        this.propertyOverrideMap = new HashMap<>();
    }

    public Map<String, Serializable> getPropertyOverrideMap() {
        return propertyOverrideMap;
    }
}
