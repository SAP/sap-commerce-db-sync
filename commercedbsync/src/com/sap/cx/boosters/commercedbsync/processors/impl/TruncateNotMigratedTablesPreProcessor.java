package com.sap.cx.boosters.commercedbsync.processors.impl;

import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sap.cx.boosters.commercedbsync.context.CopyContext;
import com.sap.cx.boosters.commercedbsync.context.MigrationContext;
import com.sap.cx.boosters.commercedbsync.processors.MigrationPreProcessor;
import com.sap.cx.boosters.commercedbsync.repository.DataRepository;


public class TruncateNotMigratedTablesPreProcessor implements MigrationPreProcessor
{

	private static final Logger LOG = LoggerFactory.getLogger(TruncateNotMigratedTablesPreProcessor.class);

	@Override
	public void process(final CopyContext context)
	{
		final MigrationContext migrationContext = context.getMigrationContext();
		if (migrationContext.isFullDatabaseMigration())
		{
			final Set<String> migrationItems = migrationContext.getIncludedTables();
			final DataRepository dataTargetRepository = migrationContext.getDataTargetRepository();
			try
			{
				dataTargetRepository.getAllTableNames().stream().filter(table -> !migrationItems.contains(table))
						.forEach(notMigratedTable -> {
							try
							{
								dataTargetRepository.truncateTable(notMigratedTable);
								LOG.debug("Not-migrated {} table is truncated", notMigratedTable);
							}
							catch (final Exception e)
							{
								LOG.error("Cannot truncate not-migrated table", e);
							}
						});
			}
			catch (Exception e)
			{
				LOG.error("TruncateNotMigratedTablesPreprocessor is failed", e);
			}
		}
	}
}
