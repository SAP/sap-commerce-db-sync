/*
 *  Copyright: 2023 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.provider;

import com.sap.cx.boosters.commercedbsync.TableCandidate;
import com.sap.cx.boosters.commercedbsync.context.CopyContext;
import com.sap.cx.boosters.commercedbsync.context.MigrationContext;

import java.util.Set;

/**
 * Provides the means to copy an Item fro Source to Target
 */
public interface CopyItemProvider {
    String TYPE_SYSTEM_PROPS_TABLE = "typesystemprops";
    String[] TYPE_SYSTEM_RELATED_TYPES = new String[]{"atomictypes", "attributeDescriptors", "collectiontypes",
            "composedtypes", "enumerationvalues", "maptypes", TYPE_SYSTEM_PROPS_TABLE};
    Set<CopyContext.DataCopyItem> get(MigrationContext context) throws Exception;

    Set<TableCandidate> getSourceTableCandidates(MigrationContext context) throws Exception;

    Set<TableCandidate> getTargetTableCandidates(MigrationContext context) throws Exception;
}
