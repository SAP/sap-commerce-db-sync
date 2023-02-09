package com.sap.cx.boosters.commercedbsync.processors.impl;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.ddlutils.model.Database;
import org.apache.ddlutils.model.Index;
import org.apache.ddlutils.model.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sap.cx.boosters.commercedbsync.context.CopyContext;
import com.sap.cx.boosters.commercedbsync.context.MigrationContext;
import com.sap.cx.boosters.commercedbsync.processors.MigrationPostProcessor;
import com.sap.cx.boosters.commercedbsync.repository.DataRepository;

import de.hybris.platform.servicelayer.config.ConfigurationService;


public class IndexAlignerPostProcessor implements MigrationPostProcessor
{

	private static final Logger LOG = LoggerFactory.getLogger(IndexAlignerPostProcessor.class);

	private ConfigurationService configurationService;

	@Override
	public void process(final CopyContext context)
	{
		final MigrationContext migrationContext = context.getMigrationContext();
		if (migrationContext.isDropAllIndexesEnabled())
		{
			LOG.info("Aligning indexes on target...");
			final DataRepository dataTargetRepository = migrationContext.getDataTargetRepository();
			final String indiciesSQL = generateAlterTablesSql(migrationContext);
			indiciesSQL.lines().forEach(indexSQL -> {
				if(StringUtils.isNotBlank(indexSQL))
				{
					LOG.info("Executing {}", indexSQL);
					try
					{
						dataTargetRepository.executeUpdateAndCommit(indexSQL);
					}
					catch (Exception e)
					{
						LOG.error("Execution failed for " + indexSQL, e);
					}
				}
			});
			LOG.info("Index alignment on target is completed.");
		}
	}

	private String generateAlterTablesSql(final MigrationContext migrationContext)
	{
		final Database sourceDatabase = migrationContext.getDataSourceRepository().asDatabase();
		final DataRepository dataTargetRepository = migrationContext.getDataTargetRepository();
		final Database targetDatabase = dataTargetRepository.asDatabase();
		final Set<String> excludedIndices = getExcludedIndicies();

		for (final String copiedTable : migrationContext.getIncludedTables())
		{
			final Table sourceTable = sourceDatabase.findTable(copiedTable);
			final Table targetTable = targetDatabase.findTable(copiedTable);
			if (sourceTable != null && targetTable != null)
			{
				final Index[] sourceTableIndices = sourceTable.getIndices();
				final Index[] targetTableIndices = targetTable.getIndices();
				for (final Index sourceTableIndex : sourceTableIndices)
				{
					if (!ArrayUtils.contains(targetTableIndices, sourceTableIndex)
							&& !excludedIndices.contains((sourceTable.getName() + "." + sourceTableIndex.getName()).toLowerCase()))
					{
						LOG.debug("Found missing index {} for {}", sourceTableIndex, copiedTable);
						targetTable.addIndex(sourceTableIndex);
					}
				}
			}
			else
			{
				LOG.warn("Table {} is not found one of the databases: source[{}], target[{}]", copiedTable, sourceTable, targetTable);
			}
		}

		final String alterTablesSql = dataTargetRepository.asPlatform().getAlterTablesSql(targetDatabase);
		LOG.debug("Generated alter table sql for missing indexes: {}", alterTablesSql);
		return alterTablesSql;
	}

	private Set<String> getExcludedIndicies()
	{
		final String excludedIndiciesStr = configurationService.getConfiguration()
				.getString("migration.data.indices.drop.recreate.exclude");

		return Arrays.stream(excludedIndiciesStr.split(",")).map(String::toLowerCase).collect(Collectors.toSet());
	}

	public void setConfigurationService(final ConfigurationService configurationService)
	{
		this.configurationService = configurationService;
	}
}
