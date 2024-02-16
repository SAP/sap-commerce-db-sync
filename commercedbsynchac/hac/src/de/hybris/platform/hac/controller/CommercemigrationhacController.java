/*
 *  Copyright: 2023 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package de.hybris.platform.hac.controller;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import de.hybris.platform.commercedbsynchac.data.*;
import de.hybris.platform.servicelayer.config.ConfigurationService;
import de.hybris.platform.servicelayer.user.UserService;
import org.apache.commons.lang.BooleanUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.util.Strings;
import com.sap.cx.boosters.commercedbsync.MigrationStatus;
import com.sap.cx.boosters.commercedbsync.constants.CommercedbsyncConstants;
import com.sap.cx.boosters.commercedbsync.context.LaunchOptions;
import com.sap.cx.boosters.commercedbsync.context.MigrationContext;
import com.sap.cx.boosters.commercedbsync.logging.JDBCQueriesStore;
import com.sap.cx.boosters.commercedbsync.repository.DataRepository;
import com.sap.cx.boosters.commercedbsync.service.DatabaseMigrationService;
import com.sap.cx.boosters.commercedbsync.service.DatabaseSchemaDifferenceService;
import com.sap.cx.boosters.commercedbsync.service.impl.BlobDatabaseMigrationReportStorageService;
import com.sap.cx.boosters.commercedbsync.service.impl.DefaultDatabaseSchemaDifferenceService;
import com.sap.cx.boosters.commercedbsync.utils.MaskUtil;
import com.sap.cx.boosters.commercedbsynchac.metric.MetricService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ClassPathResource;
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
import java.io.Serializable;
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

    private static final String DEFAULT_EMPTY_VAL = "[NOT SET]";
    private static final boolean DEFAULT_BOOLEAN_VAL = false;
    private static final Logger LOG = LoggerFactory.getLogger(CommercemigrationhacController.class);
    private static final SimpleDateFormat DATE_TIME_FORMATTER = new SimpleDateFormat("YYYY-MM-dd HH:mm",
            Locale.ENGLISH);

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
    private MetricService metricService;

    @Autowired
    BlobDatabaseMigrationReportStorageService blobDatabaseMigrationReportStorageService;

    private final boolean useCodeMirrorWebJar;

    public CommercemigrationhacController() {
        useCodeMirrorWebJar = (new ClassPathResource("META-INF/maven/org.webjars.npm/codemirror/pom.properties"))
                .isReadable();
    }

    @ModelAttribute("useCodeMirrorWebJar")
    public boolean isUseCodeMirrorWebJar() {
        return useCodeMirrorWebJar;
    }

    @RequestMapping(value = {"/migrationSchema"}, method = {org.springframework.web.bind.annotation.RequestMethod.GET})
    public String schema(final Model model) {
        logAction("Schema migration tab clicked");
        // ORACLE_TARGET -- start
        migrationContext.refreshSelf();
        // ORACLE_TARGET -- END
        model.addAttribute("wikiJdbcLogging", "some notes  on database");
        model.addAttribute("wikiDatabase", "some more note on supported features");
        Map<String, Boolean> schemaSettings = new HashMap<>();
        schemaSettings.put(CommercedbsyncConstants.MIGRATION_SCHEMA_TARGET_COLUMNS_ADD_ENABLED,
                migrationContext.isAddMissingColumnsToSchemaEnabled());
        schemaSettings.put(CommercedbsyncConstants.MIGRATION_SCHEMA_TARGET_TABLES_REMOVE_ENABLED,
                migrationContext.isRemoveMissingTablesToSchemaEnabled());
        schemaSettings.put(CommercedbsyncConstants.MIGRATION_SCHEMA_TARGET_TABLES_ADD_ENABLED,
                migrationContext.isAddMissingTablesToSchemaEnabled());
        schemaSettings.put(CommercedbsyncConstants.MIGRATION_SCHEMA_TARGET_COLUMNS_REMOVE_ENABLED,
                migrationContext.isRemoveMissingColumnsToSchemaEnabled());
        model.addAttribute("schemaSettings", schemaSettings);
        model.addAttribute("schemaMigrationDisabled", !migrationContext.isSchemaMigrationEnabled());
        model.addAttribute("schemaSqlForm", new SchemaSqlFormData());
        return "schemaCopy";
    }

    @RequestMapping(value = {"/migrationData"}, method = {org.springframework.web.bind.annotation.RequestMethod.GET})
    public String data(final Model model) {
        logAction("Data migration tab clicked");
        // ORACLE_TARGET -- start
        // migrationContext.refreshSelf();
        model.addAttribute("isTimezoneEqual", checkTimeZoneDifferences(migrationContext));
        model.addAttribute("isIncremental", migrationContext.isIncrementalModeEnabled());
        Instant timestamp = migrationContext.getIncrementalTimestamp();
        model.addAttribute("incrementalTimestamp", timestamp == null ? DEFAULT_EMPTY_VAL : timestamp);
        model.addAttribute("srcTsName",
                StringUtils.defaultIfEmpty(
                        migrationContext.getDataSourceRepository().getDataSourceConfiguration().getTypeSystemName(),
                        DEFAULT_EMPTY_VAL));
        model.addAttribute("tgtTsName",
                StringUtils.defaultIfEmpty(
                        migrationContext.getDataTargetRepository().getDataSourceConfiguration().getTypeSystemName(),
                        DEFAULT_EMPTY_VAL));
        model.addAttribute("srcPrefix",
                StringUtils.defaultIfEmpty(
                        migrationContext.getDataSourceRepository().getDataSourceConfiguration().getTablePrefix(),
                        DEFAULT_EMPTY_VAL));
        model.addAttribute("tgtMigPrefix",
                StringUtils.defaultIfEmpty(
                        migrationContext.getDataTargetRepository().getDataSourceConfiguration().getTablePrefix(),
                        DEFAULT_EMPTY_VAL));
        model.addAttribute("tgtActualPrefix", StringUtils.defaultIfEmpty(
                configurationService.getConfiguration().getString("db.tableprefix"), DEFAULT_EMPTY_VAL));
        model.addAttribute("isLogSql",
                BooleanUtils.toBooleanDefaultIfNull(migrationContext.isLogSql(), DEFAULT_BOOLEAN_VAL));
        model.addAttribute("isSchedulerResumeEnabled", migrationContext.isSchedulerResumeEnabled());
        model.addAttribute("isDataExportEnabled", migrationContext.isDataExportEnabled());
        return "dataCopy";
    }

    @RequestMapping(value = {"/migrationDataSource"}, method = {
            org.springframework.web.bind.annotation.RequestMethod.GET})
    public String dataSource(final Model model) {
        logAction("Data sources tab clicked");
        model.addAttribute("wikiJdbcLogging", "some notes  on database");
        model.addAttribute("wikiDatabase", "some more note on supported features");
        return "dataSource";
    }

    @RequestMapping(value = {"/migrationDataSource/{profile}"}, method = {
            org.springframework.web.bind.annotation.RequestMethod.GET})
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
            dataSourceConfigurationData.setConnectionString(
                    MaskUtil.stripJdbcPassword(dataRepository.getDataSourceConfiguration().getConnectionString()));
            dataSourceConfigurationData.setUserName(dataRepository.getDataSourceConfiguration().getUserName());
            dataSourceConfigurationData
                    .setPassword(dataRepository.getDataSourceConfiguration().getPassword().replaceAll(".*", "*"));
            dataSourceConfigurationData.setCatalog(dataRepository.getDataSourceConfiguration().getCatalog());
            dataSourceConfigurationData.setSchema(dataRepository.getDataSourceConfiguration().getSchema());
            dataSourceConfigurationData.setMaxActive(dataRepository.getDataSourceConfiguration().getMaxActive());
            dataSourceConfigurationData.setMaxIdle(dataRepository.getDataSourceConfiguration().getMaxIdle());
            dataSourceConfigurationData.setMinIdle(dataRepository.getDataSourceConfiguration().getMinIdle());
            dataSourceConfigurationData
                    .setRemoveAbandoned(dataRepository.getDataSourceConfiguration().isRemoveAbandoned());
        }

        return dataSourceConfigurationData;
    }

    @RequestMapping(value = {"/migrationDataSource/{profile}/validate"}, method = {
            org.springframework.web.bind.annotation.RequestMethod.GET})
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
        if (StringUtils.equalsIgnoreCase(profile,
                migrationContext.getDataSourceRepository().getDataSourceConfiguration().getProfile())) {
            return migrationContext.getDataSourceRepository();
        } else if (StringUtils.equalsIgnoreCase(profile,
                migrationContext.getDataTargetRepository().getDataSourceConfiguration().getProfile())) {
            return migrationContext.getDataTargetRepository();
        } else {
            return null;
        }
    }

    @RequestMapping(value = {"/generateSchemaScript"}, method = {
            org.springframework.web.bind.annotation.RequestMethod.GET})
    @ResponseBody
    public String generateSchemaScript() throws Exception {
        logAction("Generate schema script button clicked");
        // ORACLE_TARGET -- start
        migrationContext.refreshSelf();
        // ORACLE_TARGET -- END
        return databaseSchemaDifferenceService.generateSchemaDifferencesSql(migrationContext);
    }

    @RequestMapping(value = {"/migrateSchema"}, method = {org.springframework.web.bind.annotation.RequestMethod.POST})
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

    @RequestMapping(value = {"/previewSchemaMigration"}, method = {
            org.springframework.web.bind.annotation.RequestMethod.GET})
    @ResponseBody
    public SchemaDifferenceResultContainerData previewSchemaMigration() throws Exception {
        logAction("Preview schema migration changes button clicked");
        LOG.info("Starting preview of source and target db diff...");
        DefaultDatabaseSchemaDifferenceService.SchemaDifferenceResult difference = databaseSchemaDifferenceService
                .getDifference(migrationContext);
        SchemaDifferenceResultData sourceSchemaDifferenceResultData = getSchemaDifferenceResultData(
                difference.getSourceSchema());
        SchemaDifferenceResultData targetSchemaDifferenceResultData = getSchemaDifferenceResultData(
                difference.getTargetSchema());
        SchemaDifferenceResultContainerData schemaDifferenceResultContainerData = new SchemaDifferenceResultContainerData();
        schemaDifferenceResultContainerData.setSource(sourceSchemaDifferenceResultData);
        schemaDifferenceResultContainerData.setTarget(targetSchemaDifferenceResultData);

        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        String timeStamp = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss").format(new Date());
        try {
            InputStream is = new ByteArrayInputStream(
                    gson.toJson(schemaDifferenceResultContainerData).getBytes(StandardCharsets.UTF_8));
            blobDatabaseMigrationReportStorageService.store("schema-differences-" + timeStamp + ".json", is);
        } catch (Exception e) {
            LOG.error("Failed to save the schema differences report to blob storage!");
        }
        return schemaDifferenceResultContainerData;
    }

    private SchemaDifferenceResultData getSchemaDifferenceResultData(
            DefaultDatabaseSchemaDifferenceService.SchemaDifference diff) {
        SchemaDifferenceResultData schemaDifferenceResultData = new SchemaDifferenceResultData();

        Map<String, String> missingTablesMap = diff.getMissingTables().stream()
                .collect(Collectors.toMap(e -> getTableName(diff, e.getRightName()), e -> ""));
        Map<String, String> missingColumnsMap = diff.getMissingColumnsInTable().asMap().entrySet().stream()
                .collect(Collectors.toMap(e -> getTableName(diff, e.getKey().getRightName()),
                        e -> Joiner.on(";").join(e.getValue())));

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

    @RequestMapping(value = "/copyData", method = RequestMethod.POST, produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseBody
    public MigrationStatus copyData(@RequestParam Map<String, Serializable> copyConfig) throws Exception {
        if (migrationContext.isDataExportEnabled()) {
            throw new IllegalStateException("Migration cannot be started from HAC");
        }

        String currentMigrationId = databaseMigrationService.getMigrationID(migrationContext);
        MigrationStatus migrationStatus = new MigrationStatus();
        LaunchOptions launchOptions = new LaunchOptions();
        launchOptions.getPropertyOverrideMap().putAll(copyConfig);
        Serializable isResume = copyConfig.getOrDefault(CommercedbsyncConstants.MIGRATION_SCHEDULER_RESUME_ENABLED,
                false);

        // ORACLE_TARGET -- start
        migrationContext.refreshSelf();
        // ORACLE_TARGET -- END

        try {
            if (BooleanUtils.toBoolean(isResume.toString()) && StringUtils.isNotEmpty(currentMigrationId)) {
                logAction("Resume data migration executed");

                databaseMigrationService.resumeUnfinishedMigration(migrationContext, launchOptions, currentMigrationId);
            } else {
                logAction("Start data migration executed");

                currentMigrationId = databaseMigrationService.startMigration(migrationContext, launchOptions);
            }
        } catch (Exception e) {
            migrationStatus.setCustomException(e.getMessage());

            return migrationStatus;
        } finally {
            copyConfig.replace(CommercedbsyncConstants.MIGRATION_SCHEDULER_RESUME_ENABLED, false);
        }

        return databaseMigrationService.getMigrationState(migrationContext, currentMigrationId);
    }

    @RequestMapping(value = "/abortCopy", method = RequestMethod.PUT, produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseBody
    public String abortCopy(@RequestBody String migrationID) throws Exception {
        if (migrationContext.isDataExportEnabled()) {
            throw new IllegalStateException("Migration cannot be aborted from HAC");
        }

        logAction("Stop data migration executed");
        // ORACLE_TARGET -- start
        migrationContext.refreshSelf();
        // ORACLE_TARGET -- END
        databaseMigrationService.stopMigration(migrationContext, migrationID);
        return "true";
    }

    @GetMapping(value = "/resumeRunning")
    @ResponseBody
    public MigrationStatus resumeRunning() throws Exception {
        final String currentMigrationId = databaseMigrationService.getMigrationID(migrationContext);
        if (StringUtils.isNotEmpty(currentMigrationId)) {
            MigrationStatus migrationState = databaseMigrationService.getMigrationState(migrationContext,
                    currentMigrationId);
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
        MigrationStatus migrationState = databaseMigrationService.getMigrationState(migrationContext, migrationID,
                sinceTime);
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

    @GetMapping(value = "/copyReport", produces = MediaType.APPLICATION_OCTET_STREAM_VALUE)
    public @ResponseBody byte[] getCopyReport(@RequestParam String migrationId, HttpServletResponse response)
            throws Exception {
        logAction("Download migration report button clicked");
        response.setHeader("Content-Disposition", "attachment; filename=migration-report.json");
        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        String json = gson.toJson(databaseMigrationService.getMigrationReport(migrationContext, migrationId));
        return json.getBytes(StandardCharsets.UTF_8.name());
    }

    @GetMapping(value = "/dataSourceJdbcReport", produces = MediaType.APPLICATION_OCTET_STREAM_VALUE)
    public @ResponseBody byte[] getDataSourceJdbcReport(@RequestParam String migrationId,
            HttpServletResponse response) {
        logAction("Download data source jdbc queries report button clicked");
        JDBCQueriesStore sourceEntriesStore = migrationContext.getDataSourceRepository().getJdbcQueriesStore();
        return getLogFile(migrationId, response, sourceEntriesStore);
    }

    @GetMapping(value = "/dataTargetJdbcReport", produces = MediaType.APPLICATION_OCTET_STREAM_VALUE)
    public @ResponseBody byte[] getDataTargetJdbcReport(@RequestParam String migrationId,
            HttpServletResponse response) {
        logAction("Download data target jdbc queries report button clicked");
        JDBCQueriesStore targetEntriesStore = migrationContext.getDataTargetRepository().getJdbcQueriesStore();
        return getLogFile(migrationId, response, targetEntriesStore);
    }

    private byte[] getLogFile(String migrationId, HttpServletResponse response, JDBCQueriesStore jdbcQueriesStore) {
        Pair<byte[], String> logFilePair = jdbcQueriesStore.getLogFile(migrationId);
        final byte[] logFileBytes = logFilePair.getLeft();
        final String logFileName = logFilePair.getRight();
        response.setHeader("Content-Disposition", "attachment; filename=" + logFileName);
        return logFileBytes;
    }

    @RequestMapping(value = "/metrics", method = RequestMethod.GET)
    @ResponseBody
    public List<MetricData> getMetrics() throws Exception {
        return metricService.getMetrics(migrationContext);
    }

    private void logAction(String message) {
        LOG.info("{}: {} - User:{} - Time:{}", "CMT Action", message, userService.getCurrentUser().getUid(),
                LocalDateTime.now());
    }

    @RequestMapping(value = {"/loadMigrationReports"}, method = {
            org.springframework.web.bind.annotation.RequestMethod.GET})
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
        if (blob != null && blob.getProperties() != null) {
            Date lastModified = blob.getProperties().getLastModified();
            if (lastModified != null) {
                return DATE_TIME_FORMATTER.format(lastModified);
            }
        }
        return Strings.EMPTY;
    }

    @GetMapping(value = "/downloadLogsReport", produces = MediaType.APPLICATION_OCTET_STREAM_VALUE)
    public @ResponseBody ResponseEntity<byte[]> downloadLogsReport(@RequestParam String migrationId) throws Exception {
        logAction("Download migration report button clicked");
        byte[] outputFile = blobDatabaseMigrationReportStorageService.getReport(migrationId);
        HttpHeaders responseHeaders = new HttpHeaders();
        responseHeaders.set("Content-Type", "text/plain; charset=utf-8");
        responseHeaders.setContentLength(outputFile.length);
        responseHeaders.set("Content-disposition", "attachment; filename=migration-report.json");
        return new ResponseEntity<>(outputFile, responseHeaders, HttpStatus.OK);
    }

    @RequestMapping(value = {"/migrationReports"}, method = {org.springframework.web.bind.annotation.RequestMethod.GET})
    public String reports(final Model model) {
        logAction("Migration reports tab clicked");
        return "migrationReports";
    }

    @RequestMapping(value = {"/configPanel"}, method = {org.springframework.web.bind.annotation.RequestMethod.GET})
    public @ResponseBody ConfigPanelDTO configPanel(final Model model) {
        ConfigPanelDTO configPanelDTO = new ConfigPanelDTO();
        ConfigPanelItemDTO resume = createConfigItem("resumeUnfinishedItems", "Resume Mode",
                "If enabled, resumes next migration from where it was stopped", Boolean.class,
                migrationContext.isSchedulerResumeEnabled(), "true",
                CommercedbsyncConstants.MIGRATION_SCHEDULER_RESUME_ENABLED);
        ConfigPanelItemDTO parTables = createConfigItem("maxParallelTableCopy", "Parallel Tables",
                "Number of tables to be copied in parallel", Integer.class, migrationContext.getMaxParallelTableCopy(),
                "true", CommercedbsyncConstants.MIGRATION_DATA_MAXPRALLELTABLECOPY);
        ConfigPanelItemDTO maxReader = createConfigItem("maxReaderWorkers", "Reader Workers",
                "Number of reader workers to be used for each table", Integer.class,
                migrationContext.getMaxParallelReaderWorkers(), "true",
                CommercedbsyncConstants.MIGRATION_DATA_WORKERS_READER_MAXTASKS);
        ConfigPanelItemDTO maxWriter = createConfigItem("maxWriterWorkers", "Writer Workers",
                "Number of writer workers to be used for each table", Integer.class,
                migrationContext.getMaxParallelWriterWorkers(), "true",
                CommercedbsyncConstants.MIGRATION_DATA_WORKERS_WRITER_MAXTASKS);
        ConfigPanelItemDTO batchSize = createConfigItem("batchSize", "Batch Size", "Batch size used to query data",
                Integer.class, migrationContext.getReaderBatchSize(), "${!getValueByItemId('resumeUnfinishedItems')}",
                CommercedbsyncConstants.MIGRATION_DATA_READER_BATCHSIZE);
        configPanelDTO.setItems(Lists.newArrayList(resume, parTables, maxReader, maxWriter, batchSize));
        return configPanelDTO;
    }

    private ConfigPanelItemDTO createConfigItem(String id, String name, String description, Class type,
            Object initialValue, String renderIf, String propertyBinding) {
        ConfigPanelItemDTO configPanelItemDTO = new ConfigPanelItemDTO();
        configPanelItemDTO.setId(id);
        configPanelItemDTO.setName(name);
        configPanelItemDTO.setDescription(description);
        configPanelItemDTO.setType(type);
        configPanelItemDTO.setInitialValue(initialValue);
        configPanelItemDTO.setRenderIf(renderIf);
        configPanelItemDTO.setPropertyBinding(propertyBinding);
        return configPanelItemDTO;
    }

    private boolean checkTimeZoneDifferences(MigrationContext context) {
        String databaseTimezone = context.getDataSourceRepository().getDatabaseTimezone();

        if (StringUtils.isEmpty(databaseTimezone)) {
            LOG.info("Database timezone for source not available!");
            return false;
        }

        TimeZone source = TimeZone.getTimeZone(databaseTimezone);
        if (TimeZone.getTimeZone("UTC").getRawOffset() == source.getRawOffset()) {
            LOG.info("The timezone on source and target are the same!!");
            return true;
        }
        LOG.info("The timezone on source and target are different!!");
        return false;
    }
}
