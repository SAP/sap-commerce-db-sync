/*
 *  Copyright: 2022 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.context.impl;

import com.sap.cx.boosters.commercedbsync.constants.CommercedbsyncConstants;
import com.sap.cx.boosters.commercedbsync.profile.DataSourceConfiguration;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import com.sap.cx.boosters.commercedbsync.context.IncrementalMigrationContext;
import com.sap.cx.boosters.commercedbsync.repository.impl.DataRepositoryFactory;

import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Collections;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

public class DefaultIncrementalMigrationContext extends DefaultMigrationContext implements IncrementalMigrationContext {

    private static final Logger LOG = Logger.getLogger(DefaultIncrementalMigrationContext.class.getName());
    private Instant timestampInstant;
    private Set<String> incrementalTables;
    private Set<String> includedTables;


    public DefaultIncrementalMigrationContext(DataSourceConfiguration sourceDataSourceConfiguration, DataSourceConfiguration targetDataSourceConfiguration, DataRepositoryFactory dataRepositoryFactory, Configuration configuration) throws Exception {
        super(sourceDataSourceConfiguration, targetDataSourceConfiguration, dataRepositoryFactory, configuration);
    }

    @Override
    public Instant getIncrementalMigrationTimestamp() {
        return timestampInstant;
    }

    @Override
    public void setSchemaMigrationAutoTriggerEnabled(boolean autoTriggerEnabled) {
        configuration.setProperty(CommercedbsyncConstants.MIGRATION_SCHEMA_AUTOTRIGGER_ENABLED,
                String.valueOf(autoTriggerEnabled));
    }

    @Override
    public void setTruncateEnabled(boolean truncateEnabled) {
        configuration.setProperty(CommercedbsyncConstants.MIGRATION_DATA_TRUNCATE_ENABLED,
                String.valueOf(truncateEnabled));
    }

    @Override
    public void setIncrementalMigrationTimestamp(Instant timeStampInstant) {
        this.timestampInstant = timeStampInstant;
    }

    @Override
    public Set<String> setIncrementalTables(Set<String> incrementalTables) {
        return this.incrementalTables = incrementalTables;
    }


    @Override
    public Set<String> getIncrementalTables() {
        return CollectionUtils.isNotEmpty(this.incrementalTables) ?
                this.incrementalTables : getListProperty(CommercedbsyncConstants.MIGRATION_DATA_INCREMENTAL_TABLES);
    }

    @Override
    public void setIncrementalModeEnabled(boolean incrementalModeEnabled) {
        configuration.setProperty(CommercedbsyncConstants.MIGRATION_DATA_INCREMENTAL_ENABLED,
                Boolean.toString(incrementalModeEnabled));
    }


    @Override
    public Instant getIncrementalTimestamp() {
        if (null != getIncrementalMigrationTimestamp()) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Here getIncrementalTimestamp(): " + timestampInstant);
            }
            return getIncrementalMigrationTimestamp();
        }
        String timeStamp = getStringProperty(CommercedbsyncConstants.MIGRATION_DATA_INCREMENTAL_TIMESTAMP);
        if (StringUtils.isEmpty(timeStamp)) {
            return null;
        }
        return ZonedDateTime.parse(timeStamp, DateTimeFormatter.ISO_ZONED_DATE_TIME).toInstant();
    }

    @Override
    public Set<String> getIncludedTables() {
        if (isIncrementalModeEnabled()) {
            return Collections.emptySet();
        }
        return CollectionUtils.isNotEmpty(includedTables) ? includedTables :
                getListProperty(CommercedbsyncConstants.MIGRATION_DATA_TABLES_INCLUDED);
    }

    @Override
    public void setIncludedTables(Set<String> includedTables) {
        this.includedTables = includedTables;
    }

    @Override
    public void setDeletionEnabled(boolean deletionEnabled) {
        this.deletionEnabled = deletionEnabled;
    }

    @Override
    public void setLpTableMigrationEnabled(boolean lpTableMigrationEnabled) {
        this.lpTableMigrationEnabled = lpTableMigrationEnabled;
    }

    private Set<String> getListProperty(final String key) {
        final String tables = super.configuration.getString(key);

        if (StringUtils.isEmpty(tables)) {
            return Collections.emptySet();
        }

        final Set<String> result = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        final String[] tablesArray = tables.split(",");
        result.addAll(Arrays.stream(tablesArray).collect(Collectors.toSet()));

        return result;
    }
	// ORACLE_TARGET -- START
	/*
	 * Fire this method only from HAC controller...not from the jobs.
	 */
	@Override
	public void refreshSelf()
	{
		LOG.info("Refreshing Context");
		// lists
		this.setIncludedTables(Collections.emptySet());
		this.setIncrementalTables(Collections.emptySet());
		this.setIncrementalMigrationTimestamp(null);
	}
	// ORACLE_TARGET -- END
}
