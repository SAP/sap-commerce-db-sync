/*
 *  Copyright: 2022 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */
package de.hybris.platform.hac.controller;

import com.google.common.base.Joiner;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import de.hybris.platform.commercedbsynchac.data.*;
import de.hybris.platform.servicelayer.config.ConfigurationService;
import de.hybris.platform.servicelayer.user.UserService;
import org.apache.commons.lang.BooleanUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.logging.log4j.util.Strings;
import com.sap.cx.boosters.commercedbsync.MigrationStatus;
import com.sap.cx.boosters.commercedbsync.constants.CommercedbsyncConstants;
import com.sap.cx.boosters.commercedbsync.context.MigrationContext;
import com.sap.cx.boosters.commercedbsync.repository.DataRepository;
import com.sap.cx.boosters.commercedbsync.service.DatabaseMigrationService;
import com.sap.cx.boosters.commercedbsync.service.DatabaseMigrationSynonymService;
import com.sap.cx.boosters.commercedbsync.service.DatabaseSchemaDifferenceService;
import com.sap.cx.boosters.commercedbsync.service.impl.BlobDatabaseMigrationReportStorageService;
import com.sap.cx.boosters.commercedbsync.service.impl.DefaultDatabaseSchemaDifferenceService;
import com.sap.cx.boosters.commercedbsync.utils.MaskUtil;
import com.sap.cx.boosters.commercedbsynchac.metric.MetricService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletResponse;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.*;
import java.util.stream.Collectors;


/**
 *
 */
@Controller
@RequestMapping("/commercedbsynchac/**")
public class CommercemigrationhacController {

    public static final String DEFAULT_EMPTY_VAL = "[NOT SET]";
    private static final Logger LOG = LoggerFactory.getLogger(CommercemigrationhacController.class);
    private static final SimpleDateFormat DATE_TIME_FORMATTER = new SimpleDateFormat("YYYY-MM-dd HH:mm", Locale.ENGLISH);

    @Autowired
    private UserService userService;

    @Autowired
    private DatabaseMigrationService databaseMigrationService;

    @Autowired
    private DatabaseSchemaDifferenceService databaseSchemaDifferenceService;

    @Autowired
    private ConfigurationService configurationService;

    @Autowired
    private MigrationContext migrationContext;

    @Autowired
    private DatabaseMigrationSynonymService databaseMigrationSynonymService;

    @Autowired
    private MetricService metricService;

    @Autowired
    BlobDatabaseMigrationReportStorageService blobDatabaseMigrationReportStorageService;


    private String currentMigrationId;

    @RequestMapping(value =
            {"/migrationSchema"}, method =
            {org.springframework.web.bind.annotation.RequestMethod.GET})
    public String schema(final Model model) {
        logAction("Schema migration tab clicked");
		// ORACLE_TARGET -- start
		migrationContext.refreshSelf();
		// ORACLE_TARGET -- END
        model.addAttribute("wikiJdbcLogging", "some notes  on database");
        model.addAttribute("wikiDatabase", "some more note on supported features");
        Map<String, Boolean> schemaSettings = new HashMap<>();
        schemaSettings.put(CommercedbsyncConstants.MIGRATION_SCHEMA_TARGET_COLUMNS_ADD_ENABLED, migrationContext.isAddMissingColumnsToSchemaEnabled());
        schemaSettings.put(CommercedbsyncConstants.MIGRATION_SCHEMA_TARGET_TABLES_REMOVE_ENABLED, migrationContext.isRemoveMissingTablesToSchemaEnabled());
        schemaSettings.put(CommercedbsyncConstants.MIGRATION_SCHEMA_TARGET_TABLES_ADD_ENABLED, migrationContext.isAddMissingTablesToSchemaEnabled());
        schemaSettings.put(CommercedbsyncConstants.MIGRATION_SCHEMA_TARGET_COLUMNS_REMOVE_ENABLED, migrationContext.isRemoveMissingColumnsToSchemaEnabled());
        model.addAttribute("schemaSettings", schemaSettings);
        model.addAttribute("schemaMigrationDisabled", !migrationContext.isSchemaMigrationEnabled());
        model.addAttribute("schemaSqlForm", new SchemaSqlFormData());
        return "schemaCopy";
    }

    @RequestMapping(value =
            {"/migrationData"}, method =
            {org.springframework.web.bind.annotation.RequestMethod.GET})
    public String data(final Model model) {
        logAction("Data migration tab clicked");
		// ORACLE_TARGET -- start
		migrationContext.refreshSelf();
        model.addAttribute("isIncremental", migrationContext.isIncrementalModeEnabled());
        Instant timestamp = migrationContext.getIncrementalTimestamp();
        model.addAttribute("incrementalTimestamp", timestamp == null ? DEFAULT_EMPTY_VAL : timestamp);
        model.addAttribute("srcTsName", StringUtils.defaultIfEmpty(migrationContext.getDataSourceRepository().getDataSourceConfiguration().getTypeSystemName(), DEFAULT_EMPTY_VAL));
        model.addAttribute("tgtTsName", StringUtils.defaultIfEmpty(migrationContext.getDataTargetRepository().getDataSourceConfiguration().getTypeSystemName(), DEFAULT_EMPTY_VAL));
        model.addAttribute("srcPrefix", StringUtils.defaultIfEmpty(migrationContext.getDataSourceRepository().getDataSourceConfiguration().getTablePrefix(), DEFAULT_EMPTY_VAL));
        model.addAttribute("tgtMigPrefix", StringUtils.defaultIfEmpty(migrationContext.getDataTargetRepository().getDataSourceConfiguration().getTablePrefix(), DEFAULT_EMPTY_VAL));
        model.addAttribute("tgtActualPrefix", StringUtils.defaultIfEmpty(configurationService.getConfiguration().getString("db.tableprefix"), DEFAULT_EMPTY_VAL));
        return "dataCopy";
    }

    @RequestMapping(value = {"/migrationDataSource"}, method = {org.springframework.web.bind.annotation.RequestMethod.GET})
    public String dataSource(final Model model) {
        logAction("Data sources tab clicked");
        model.addAttribute("wikiJdbcLogging", "some notes  on database");
        model.addAttribute("wikiDatabase", "some more note on supported features");
        return "dataSource";
    }

    @RequestMapping(value = {"/migrationDataSource/{profile}"}, method = {org.springframework.web.bind.annotation.RequestMethod.GET})
    @ResponseBody
    public DataSourceConfigurationData dataSourceInfo(final Model model, @PathVariable String profile) {
        model.addAttribute("wikiJdbcLogging", "some notes  on database");
        model.addAttribute("wikiDatabase", "some more note on supported features");
        final DataRepository dataRepository = getDataRepository(profile);
        DataSourceConfigurationData dataSourceConfigurationData = null;

        if (dataRepository != null) {
            dataSourceConfigurationData = new DataSourceConfigurationData();
            dataSourceConfigurationData.setProfile(dataRepository.getDataSourceConfiguration().getProfile());
            dataSourceConfigurationData.setDriver(dataRepository.getDataSourceConfiguration().getDriver());
            dataSourceConfigurationData.setConnectionString(MaskUtil.stripJdbcPassword(dataRepository.getDataSourceConfiguration().getConnectionString()));
            dataSourceConfigurationData.setUserName(dataRepository.getDataSourceConfiguration().getUserName());
            dataSourceConfigurationData.setPassword(dataRepository.getDataSourceConfiguration().getPassword().replaceAll(".*", "*"));
            dataSourceConfigurationData.setCatalog(dataRepository.getDataSourceConfiguration().getCatalog());
            dataSourceConfigurationData.setSchema(dataRepository.getDataSourceConfiguration().getSchema());
            dataSourceConfigurationData.setMaxActive(dataRepository.getDataSourceConfiguration().getMaxActive());
            dataSourceConfigurationData.setMaxIdle(dataRepository.getDataSourceConfiguration().getMaxIdle());
            dataSourceConfigurationData.setMinIdle(dataRepository.getDataSourceConfiguration().getMinIdle());
            dataSourceConfigurationData.setRemoveAbandoned(dataRepository.getDataSourceConfiguration().isRemoveAbandoned());
        }

        return dataSourceConfigurationData;
    }

    @RequestMapping(value =
            {"/migrationDataSource/{profile}/validate"}, method =
            {org.springframework.web.bind.annotation.RequestMethod.GET})
    @ResponseBody
    public DataSourceValidationResultData dataSourceValidation(final Model model, @PathVariable String profile) {
        logAction("Validate connections button clicked");
        model.addAttribute("wikiJdbcLogging", "some notes  on database");
        model.addAttribute("wikiDatabase", "some more note on supported features");

        DataSourceValidationResultData dataSourceValidationResultData = new DataSourceValidationResultData();

        try {
            DataRepository dataRepository = getDataRepository(profile);
            if (dataRepository != null) {
                dataSourceValidationResultData.setValid(dataRepository.validateConnection());
            } else {
                dataSourceValidationResultData.setValid(false);
            }
        } catch (Exception e) {
            e.printStackTrace();
            dataSourceValidationResultData.setException(e.getMessage());
        }

        return dataSourceValidationResultData;
    }

    private DataRepository getDataRepository(String profile) {
        if (StringUtils.equalsIgnoreCase(profile, migrationContext.getDataSourceRepository().getDataSourceConfiguration().getProfile())) {
            return migrationContext.getDataSourceRepository();
        } else if (StringUtils.equalsIgnoreCase(profile, migrationContext.getDataTargetRepository().getDataSourceConfiguration().getProfile())) {
            return migrationContext.getDataTargetRepository();
        } else {
            return null;
        }
    }

    @RequestMapping(value =
            {"/generateSchemaScript"}, method =
            {org.springframework.web.bind.annotation.RequestMethod.GET})
    @ResponseBody
    public String generateSchemaScript() throws Exception {
        logAction("Generate schema script button clicked");
		// ORACLE_TARGET -- start
		migrationContext.refreshSelf();
		// ORACLE_TARGET -- END
        return databaseSchemaDifferenceService.generateSchemaDifferencesSql(migrationContext);
    }

    @RequestMapping(value =
            {"/migrateSchema"}, method =
            {org.springframework.web.bind.annotation.RequestMethod.POST})
    @ResponseBody
    public String migrateSchema(@ModelAttribute("schemaSqlForm") SchemaSqlFormData data) {
        try {
            logAction("Execute script button clicked");
			// ORACLE_TARGET -- start
			migrationContext.refreshSelf();
			// ORACLE_TARGET -- END
            if (BooleanUtils.isTrue(data.getAccepted())) {
                databaseSchemaDifferenceService.executeSchemaDifferencesSql(migrationContext, data.getSqlQuery());
            } else {
                throw new IllegalStateException("Checkbox not accepted");
            }
        } catch (Exception e) {
            return ExceptionUtils.getStackTrace(e);
        }
        return "Successfully executed sql";
    }

    @RequestMapping(value =
            {"/previewSchemaMigration"}, method =
            {org.springframework.web.bind.annotation.RequestMethod.GET})
    @ResponseBody
    public SchemaDifferenceResultContainerData previewSchemaMigration() throws Exception {
        logAction("Preview schema migration changes button clicked");
        LOG.info("Starting preview of source and target db diff...");
        DefaultDatabaseSchemaDifferenceService.SchemaDifferenceResult difference = databaseSchemaDifferenceService.getDifference(migrationContext);
        SchemaDifferenceResultData sourceSchemaDifferenceResultData = getSchemaDifferenceResultData(difference.getSourceSchema());
        SchemaDifferenceResultData targetSchemaDifferenceResultData = getSchemaDifferenceResultData(difference.getTargetSchema());
        SchemaDifferenceResultContainerData schemaDifferenceResultContainerData = new SchemaDifferenceResultContainerData();
        schemaDifferenceResultContainerData.setSource(sourceSchemaDifferenceResultData);
        schemaDifferenceResultContainerData.setTarget(targetSchemaDifferenceResultData);

        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        String timeStamp = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss").format(new Date());
        try {
            InputStream is = new ByteArrayInputStream(gson.toJson(schemaDifferenceResultContainerData).getBytes(StandardCharsets.UTF_8));
            blobDatabaseMigrationReportStorageService.store("schema-differences-"+timeStamp+".json", is);
        } catch (Exception e){
            LOG.error("Failed to save the schema differences report to blob storage!");
        }
        return schemaDifferenceResultContainerData;
    }

    private SchemaDifferenceResultData getSchemaDifferenceResultData(DefaultDatabaseSchemaDifferenceService.SchemaDifference diff) {
        SchemaDifferenceResultData schemaDifferenceResultData = new SchemaDifferenceResultData();

        Map<String, String> missingTablesMap = diff.getMissingTables().stream()
                .collect(Collectors.toMap(e -> getTableName(diff, e.getRightName()), e -> ""));
        Map<String, String> missingColumnsMap = diff.getMissingColumnsInTable().asMap().entrySet().stream()
                .collect(Collectors.toMap(e -> getTableName(diff, e.getKey().getRightName()), e -> Joiner.on(";").join(e.getValue())));

        Map<String, String> map = new HashMap<>();
        map.putAll(missingTablesMap);
        map.putAll(missingColumnsMap);

        String[][] result = new String[map.size()][2];
        int count = 0;
        for (Map.Entry<String, String> entry : map.entrySet()) {
            result[count][0] = entry.getKey();
            result[count][1] = entry.getValue();
            count++;
        }

        schemaDifferenceResultData.setResults(result);
        return schemaDifferenceResultData;
    }

    private String getTableName(DefaultDatabaseSchemaDifferenceService.SchemaDifference diff, String name) {
        if (StringUtils.isNotEmpty(diff.getPrefix())) {
            return String.format("%s", name);
        } else {
            return name;
        }
    }

    @RequestMapping(value = "/copyData", method = RequestMethod.PUT, produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseBody
    public MigrationStatus copyData() throws Exception {
        logAction("Start data migration executed");
		// ORACLE_TARGET -- start
		migrationContext.refreshSelf();
		// ORACLE_TARGET -- END
        this.currentMigrationId = databaseMigrationService.startMigration(migrationContext);
        return databaseMigrationService.getMigrationState(migrationContext, this.currentMigrationId);
    }

    @RequestMapping(value = "/abortCopy", method = RequestMethod.PUT, produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseBody
    public String abortCopy(@RequestBody String migrationID) throws Exception {
        logAction("Stop data migration executed");
		// ORACLE_TARGET -- start
		migrationContext.refreshSelf();
		// ORACLE_TARGET -- END
        databaseMigrationService.stopMigration(migrationContext, migrationID);
        return "true";
    }

    @RequestMapping(value = "/resumeRunning", method = RequestMethod.GET)
    @ResponseBody
    public MigrationStatus resumeRunning() throws Exception {
        if (StringUtils.isNotEmpty(this.currentMigrationId)) {
            MigrationStatus migrationState = databaseMigrationService.getMigrationState(migrationContext, this.currentMigrationId);
            prepareStateForJsonSerialization(migrationState);
            return migrationState;
        } else {
            return null;
        }
    }

    @RequestMapping(value = "/copyStatus", method = RequestMethod.GET)
    @ResponseBody
    public MigrationStatus copyStatus(@RequestParam String migrationID, @RequestParam long since) throws Exception {
        OffsetDateTime sinceTime = OffsetDateTime.ofInstant(Instant.ofEpochMilli(since), ZoneOffset.UTC);
        MigrationStatus migrationState = databaseMigrationService.getMigrationState(migrationContext, migrationID, sinceTime);
        prepareStateForJsonSerialization(migrationState);
        return migrationState;
    }

    private void prepareStateForJsonSerialization(MigrationStatus migrationState) {
        migrationState.setStartEpoch(convertToEpoch(migrationState.getStart()));
        migrationState.setStart(null);
        migrationState.setEndEpoch(convertToEpoch(migrationState.getEnd()));
        migrationState.setEnd(null);
        migrationState.setLastUpdateEpoch(convertToEpoch(migrationState.getLastUpdate()));
        migrationState.setLastUpdate(null);

        migrationState.getStatusUpdates().forEach(u -> {
            u.setLastUpdateEpoch(convertToEpoch(u.getLastUpdate()));
            u.setLastUpdate(null);
        });
    }

    private Long convertToEpoch(LocalDateTime time) {
        if (time == null) {
            return null;
        }
        return time.toInstant(ZoneOffset.UTC).toEpochMilli();
    }

    @GetMapping(
            value = "/copyReport",
            produces = MediaType.APPLICATION_OCTET_STREAM_VALUE
    )
    public @ResponseBody
    byte[] getCopyReport(@RequestParam String migrationId, HttpServletResponse response) throws Exception {
        logAction("Download migration report button clicked");
        response.setHeader("Content-Disposition", "attachment; filename=migration-report.json");
        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        String json = gson.toJson(databaseMigrationService.getMigrationReport(migrationContext, migrationId));
        return json.getBytes(StandardCharsets.UTF_8.name());
    }

    @RequestMapping(value = "/switchPrefix", method = RequestMethod.PUT)
    @ResponseBody
    public Boolean switchPrefix(@RequestParam String prefix) throws Exception {
        databaseMigrationSynonymService.recreateSynonyms(migrationContext.getDataTargetRepository(), prefix);
        return Boolean.TRUE;
    }

    @RequestMapping(value = "/metrics", method = RequestMethod.GET)
    @ResponseBody
    public List<MetricData> getMetrics() throws Exception {
        return metricService.getMetrics(migrationContext);
    }

    private void logAction(String message) {
        LOG.info("{}: {} - User:{} - Time:{}", "CMT Action", message, userService.getCurrentUser().getUid(),LocalDateTime.now());
    }

    @RequestMapping(value =
            {"/loadMigrationReports"}, method =
            {org.springframework.web.bind.annotation.RequestMethod.GET})
    @ResponseBody
    public List<ReportResultData> loadMigrationReports() {
        try {
           List<CloudBlockBlob> blobs = blobDatabaseMigrationReportStorageService.listAllReports();
           List<ReportResultData> result = new ArrayList<>();
           blobs.forEach(blob -> {
               ReportResultData reportResultData = new ReportResultData();
               reportResultData.setModifiedTimestamp(getSortableTimestamp(blob));
               reportResultData.setReportId(blob.getName());
               reportResultData.setPrimaryUri(blob.getUri().toString());
               result.add(reportResultData);
           });
           return result;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    private String getSortableTimestamp(CloudBlockBlob blob) {
        if(blob != null && blob.getProperties() != null) {
            Date lastModified = blob.getProperties().getLastModified();
            if(lastModified != null) {
                return DATE_TIME_FORMATTER.format(lastModified);
            }
        }
        return Strings.EMPTY;
    }

    @GetMapping(
            value = "/downloadLogsReport",
            produces = MediaType.APPLICATION_OCTET_STREAM_VALUE
    )
    public @ResponseBody
    ResponseEntity<byte[]> downloadLogsReport(@RequestParam String migrationId) throws Exception {
        logAction("Download migration report button clicked");
        byte[] outputFile = blobDatabaseMigrationReportStorageService.getReport(migrationId);
        HttpHeaders responseHeaders = new HttpHeaders();
        responseHeaders.set("charset", "utf-8");
        responseHeaders.setContentType(MediaType.valueOf("text/plain"));
        responseHeaders.setContentLength(outputFile.length);
        responseHeaders.set("Content-disposition", "attachment; filename=migration-report.json");
        return new ResponseEntity<>(outputFile, responseHeaders, HttpStatus.OK);
    }


    @RequestMapping(value =
            {"/migrationReports"}, method =
            {org.springframework.web.bind.annotation.RequestMethod.GET})
    public String reports(final Model model) {
        logAction("Migration reports tab clicked");
        return "migrationReports";
    }

}
