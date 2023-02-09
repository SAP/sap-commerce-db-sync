package com.sap.cx.boosters.commercedbsync.processors.impl;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.ClassPathResource;

import com.google.common.base.Splitter;
import com.sap.cx.boosters.commercedbsync.context.CopyContext;
import com.sap.cx.boosters.commercedbsync.context.MigrationContext;
import com.sap.cx.boosters.commercedbsync.processors.MigrationPreProcessor;
import com.sap.cx.boosters.commercedbsync.repository.DataRepository;

import de.hybris.bootstrap.ddl.DataBaseProvider;
import de.hybris.platform.core.model.type.ComposedTypeModel;
import de.hybris.platform.servicelayer.config.ConfigurationService;
import de.hybris.platform.servicelayer.search.FlexibleSearchService;


public class TypeInfoTableGeneratorPreProcessor implements MigrationPreProcessor
{

	private static final Logger LOG = LoggerFactory.getLogger(TypeInfoTableGeneratorPreProcessor.class);

	private FlexibleSearchService flexibleSearchService;
	private ConfigurationService configurationService;

	@Override
	public void process(final CopyContext context)
	{
		final MigrationContext migrationContext = context.getMigrationContext();
		final DataRepository dataSourceRepository = migrationContext.getDataSourceRepository();
		final String platformSpecificSQL = getPlatformSpecificSQL(dataSourceRepository.getDatabaseProvider());
		if (StringUtils.isNotBlank(platformSpecificSQL))
		{
			dataSourceRepository.runSqlScriptOnPrimary(new ClassPathResource("/sql/transformationFunctions/" + platformSpecificSQL));

			final Set<String> typeInfos = getRequiredTypeInfos();
			if (!typeInfos.isEmpty())
			{
				final Map<String, Object> params = new HashMap<>();
				params.put("composedTypes", typeInfos);
				flexibleSearchService
						.<ComposedTypeModel> search("SELECT {pk} FROM {ComposedType} WHERE {Code} IN (?composedTypes)", params)
						.getResult().stream().forEach(composedType -> {
							try
							{
								dataSourceRepository.executeUpdateAndCommitOnPrimary("INSERT INTO MIGRATIONTOOLKIT_TF_TYPEINFO VALUES ('"
										+ composedType.getCode() + "', " + composedType.getPk().getLongValue() + ")");
							}
							catch (final Exception e)
							{
								LOG.error("Cannot insert into MIGRATIONTOOLKIT_TF_TYPEINFO", e);
							}
						});
			}
		}
	}

	private String getPlatformSpecificSQL(final DataBaseProvider databaseProvider)
	{
		String platformSpecificSQL = "mssql-typeinfotable.sql";
		if (databaseProvider.isHanaUsed() || databaseProvider.isOracleUsed() || databaseProvider.isPostgreSqlUsed())
		{
			platformSpecificSQL = null;
		}

		LOG.info("Identified platform specific typeinfo table SQL {}", platformSpecificSQL);

		return platformSpecificSQL;
	}

	public Set<String> getRequiredTypeInfos()
	{
		final Set<String> types = new HashSet<>();
		final Configuration subset = configurationService.getConfiguration().subset("migration.data.t.typeinfo");
		final Iterator<String> keys = subset.getKeys();
		while (keys.hasNext())
		{
			final String current = keys.next();
			final List<String> subkeyList = Splitter.on(".").splitToList(current);
			if (subkeyList.size() == 2 && "enabled".equals(subkeyList.get(1)))
			{
				boolean val = subset.getBoolean(current, false);
				if (val)
				{
					types.add(subkeyList.get(0));
				}
			}
		}
		return types;
	}

	public void setFlexibleSearchService(final FlexibleSearchService flexibleSearchService)
	{
		this.flexibleSearchService = flexibleSearchService;
	}

	public void setConfigurationService(final ConfigurationService configurationService)
	{
		this.configurationService = configurationService;
	}
}
