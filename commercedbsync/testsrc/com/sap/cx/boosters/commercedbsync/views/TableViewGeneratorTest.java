package com.sap.cx.boosters.commercedbsync.views;

import java.util.HashSet;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.sap.cx.boosters.commercedbsync.context.MigrationContext;
import com.sap.cx.boosters.commercedbsync.repository.DataRepository;

class TableViewGeneratorTest {

	TableViewGenerator testObj;
	MigrationContext ctx;

	@Before
	void setUp() throws Exception {
		ctx = Mockito.mock(MigrationContext.class);
		DataRepository dr = Mockito.mock(DataRepository.class);
		Mockito.when(ctx.getDataSourceRepository()).thenReturn(dr);
		Mockito.when(dr.getAllColumnNames(Mockito.anyString()))
				.thenReturn(Stream.of("hjmpTS", "createdTS", "modifiedTS", "TypePkString", "OwnerPkString", "PK",
						"sealed", "p_mime", "p_size", "p_datapk", "p_location", "p_locationhash", "p_realfilename",
						"p_code", "p_internalurl", "p_description", "p_alttext", "p_removable", "p_mediaformat",
						"p_folder", "p_subfolderpath", "p_mediacontainer", "p_catalog", "p_catalogversion", "aCLTS",
						"propTS", "p_outputmimetype", "p_inputmimetype", "p_itemtimestamp", "p_format", "p_sourceitem",
						"p_fieldseparator", "p_quotecharacter", "p_commentcharacter", "p_encoding", "p_linestoskip",
						"p_removeonsuccess", "p_zipentry", "p_extractionid", "p_auditrootitem", "p_auditreportconfig",
						"p_scheduledcount", "p_cronjobpos", "p_cronjob")
						.collect(Collectors.toCollection(HashSet::new)));
		Mockito.when(ctx.getItemTypeViewNamePattern()).thenReturn("v_%s");
		Mockito.when(ctx.getViewWhereClause(Mockito.matches("medias"))).thenReturn("");
		Mockito.when(ctx.getCustomColumnsForView(Mockito.matches("medias"))).thenReturn(Map.of());
		testObj = new TableViewGenerator();		
	}

	@Test
	void testSimplestMedia() throws Exception {
		ViewConfigurationContext result = testObj.generateForTable("medias", ctx);
		Assert.assertNotNull(result);
		
	}

}
