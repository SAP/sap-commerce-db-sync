/*
 *  Copyright: 2022 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.context;

import java.time.Instant;
import java.util.Set;

/**
 * The MigrationContext contains all information needed to perform a Source -> Target Migration
 */
public interface IncrementalMigrationContext extends MigrationContext {

    Instant getIncrementalMigrationTimestamp();

    public void setSchemaMigrationAutoTriggerEnabled(final boolean autoTriggerEnabled);

    public void setTruncateEnabled(final boolean truncateEnabled);

    void setIncrementalMigrationTimestamp(final Instant timeStampInstant);

    Set<String> setIncrementalTables(final Set<String> incrementalTables);

    void setIncrementalModeEnabled(final boolean incrementalModeEnabled);

    void setIncludedTables(final Set<String> includedTables);

    public void setDeletionEnabled(boolean deletionEnabled);

    public void setLpTableMigrationEnabled(boolean lpTableMigrationEnabled);
}
