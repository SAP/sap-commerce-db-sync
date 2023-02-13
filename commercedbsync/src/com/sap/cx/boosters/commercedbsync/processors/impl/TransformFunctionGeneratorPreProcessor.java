package com.sap.cx.boosters.commercedbsync.processors.impl;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.ClassPathResource;

import com.sap.cx.boosters.commercedbsync.context.CopyContext;
import com.sap.cx.boosters.commercedbsync.context.MigrationContext;
import com.sap.cx.boosters.commercedbsync.processors.MigrationPreProcessor;
import com.sap.cx.boosters.commercedbsync.repository.DataRepository;

import de.hybris.bootstrap.ddl.DataBaseProvider;


public class TransformFunctionGeneratorPreProcessor implements MigrationPreProcessor
{

	private static final Logger LOG = LoggerFactory.getLogger(TransformFunctionGeneratorPreProcessor.class);

	@Override
	public void process(final CopyContext context)
	{
		final MigrationContext migrationContext = context.getMigrationContext();
		final DataRepository dataSourceRepository = migrationContext.getDataSourceRepository();
		final String platformSpecificSQL = getPlatformSpecificSQL(dataSourceRepository.getDatabaseProvider());
		if (StringUtils.isNotBlank(platformSpecificSQL))
		{
			dataSourceRepository.runSqlScriptOnPrimary(new ClassPathResource("/sql/transformationFunctions/" + platformSpecificSQL));
		}
	}

	private String getPlatformSpecificSQL(final DataBaseProvider databaseProvider)
	{
		String platformSpecificSQL = "-general.sql";
		switch (databaseProvider.getDbName().toLowerCase())
		{
			case "sqlserver":
				platformSpecificSQL = "mssql" + platformSpecificSQL;
				break;
			case "mysql":
				platformSpecificSQL = "mysql" + platformSpecificSQL;
				break;
			case "oracle":
				platformSpecificSQL = "oracle" + platformSpecificSQL;
				break;
			case "postgresql":
				platformSpecificSQL = "postgresql" + platformSpecificSQL;
				break;
			case "sap":
				platformSpecificSQL = "sap" + platformSpecificSQL;
				break;
			default:
				platformSpecificSQL = "hsqldb" + platformSpecificSQL;
				
		}

		LOG.info("Identified platform specific transformation function SQL {}", platformSpecificSQL);

		return platformSpecificSQL;
	}
}