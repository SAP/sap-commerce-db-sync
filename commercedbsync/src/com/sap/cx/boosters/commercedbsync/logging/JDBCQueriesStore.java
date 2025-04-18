/*
 *  Copyright: 2023 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.logging;

import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.specialized.AppendBlobClient;
import com.azure.storage.blob.specialized.BlockBlobClient;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.sap.cx.boosters.commercedbsync.repository.DataRepository;
import org.apache.commons.lang3.tuple.Pair;
import com.sap.cx.boosters.commercedbsync.context.MigrationContext;
import com.sap.cx.boosters.commercedbsync.utils.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * In-memory store containing all JDBC queries ran on a {@link DataRepository}
 * The store gets cleared from its elements after each migration
 */
public class JDBCQueriesStore {

    private static final Logger LOG = LoggerFactory.getLogger(JDBCQueriesStore.class);

    public static final String JDBCLOGS_DIRECTORY = "jdbclogs";

    private final String dbConnectionString;
    private final Collection<JdbcQueryLog> queryLogs;
    private final MigrationContext context;
    private BlobServiceClient blobServiceClient;

    private final boolean isSourceDB;
    // Unique id of the file in file storage where the jdbc store(s) across
    // cluster(s) (in multi-cluster mode) for this datasource append the JDBC
    // queries to. At the end of each migration, the post processor will create a
    // zip out of
    // this file and name it <migrationid>-[source/target]-jdbc-logs.zip
    private final String sharedStoreLogFileName;

    public JDBCQueriesStore(final String dbConnectionString, final MigrationContext context, final boolean isSourceDB) {
        this.queryLogs = Collections.synchronizedCollection(new ArrayList<>());
        this.dbConnectionString = dbConnectionString;
        this.isSourceDB = isSourceDB;
        this.sharedStoreLogFileName = isSourceDB
                ? "source-db-jdbc-store-appending-file"
                : "target-db-jdbc-store-appending-file";
        this.context = context;
    }

    /**
     * Add a JDBC query to the store
     *
     * @param newEntry
     *            new JDBC query to add to the store
     */
    public void addEntry(JdbcQueryLog newEntry) {
        if (queryLogs.size() >= context.getSqlStoreMemoryFlushThreshold()) {
            flushQueryLogsToAppendingFile();
            queryLogs.clear();
        }
        queryLogs.add(newEntry);
    }

    /**
     * Clears the store from all its JDBC queries and deletes the temporary
     * appending file
     */
    public void clear() {
        queryLogs.clear();
        resetAppendingFile();
    }

    public int getInMemoryQueryLogsCount() {
        return queryLogs.size();
    }

    /**
     * Writes the JDBC queries of the store to a log file in the file storage
     * associated with this store and compresses the log file.
     */
    public void writeToLogFileAndCompress(final String migrationId) {
        flushQueryLogsToAppendingFile();
        compressAppendingFileContent(migrationId);
        // delete the temporary appending file to start fresh on the next migration
        resetAppendingFile();
        LOG.info("Wrote JDBC Queries logs to {} in storage {} for datasource {}", getLogFileName(migrationId, true),
                context.getFileStorageConnectionString(), dbConnectionString);
    }

    public Pair<byte[], String> getLogFile(final String migrationId) {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            final BlockBlobClient zippedLogBlobFile = getZippedLogBlobFile(migrationId);
            zippedLogBlobFile.download(baos);
            return Pair.of(baos.toByteArray(), getLogFileName(migrationId, true));
        } catch (Exception e) {
            String errorMessage = String.format(
                    "Log file %s for datasource %s does not exist in storage %s or is currently being created",
                    getLogFileName(migrationId, true), dbConnectionString, context.getFileStorageContainerName());
            LOG.error(errorMessage, e);
            return Pair.of(errorMessage.getBytes(StandardCharsets.UTF_8), getLogFileName(migrationId, false));
        }
    }

    @Override
    public String toString() {
        return "JDBCEntriesInMemoryStore{" + "connectionString='" + dbConnectionString + '}';
    }

    private void flushQueryLogsToAppendingFile() {
        try {
            final AppendBlobClient sharedStoreLogFile = getSharedStoreLogFile();
            byte[] queryLogsBytes = getQueryLogsAsString().getBytes(StandardCharsets.UTF_8.name());
            try (InputStream is = new ByteArrayInputStream(queryLogsBytes)) {
                sharedStoreLogFile.appendBlock(is, queryLogsBytes.length);
            }
        } catch (Exception e) {
            LOG.error("Failed to flush querylogs to file {} in storage {} for datasource {}", sharedStoreLogFileName,
                    context.getFileStorageConnectionString(), dbConnectionString, e);
        }
    }

    private String getQueryLogsAsString() {
        // Get an array out of the elements
        // to prevent ConcurrentModificationException
        // on the SynchronizedCollection
        return Stream.of(queryLogs.toArray()).map(Object::toString).collect(Collectors.joining("\n"));
    }

    private void compressAppendingFileContent(final String migrationId) {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            final AppendBlobClient sharedStoreLogFile = getSharedStoreLogFile();
            sharedStoreLogFile.downloadStream(baos);
            byte[] zippedLogBytes = FileUtils.zipBytes(getLogFileName(migrationId, false), baos.toByteArray());
            final BlockBlobClient zippedLogBlobFile = getZippedLogBlobFile(migrationId);
            zippedLogBlobFile.upload(new ByteArrayInputStream(zippedLogBytes), zippedLogBytes.length);
        } catch (Exception e) {
            LOG.error("Failed to compress query logs from file {} in storage {} for datasource {}",
                    getLogFileName(migrationId, false), context.getFileStorageConnectionString(), dbConnectionString,
                    e);
        }
    }

    private void resetAppendingFile() {
        try {
            getSharedStoreLogFile().create(true);
        } catch (Exception e) {
            LOG.error("Failed to create or replace appending file {} in storage {} for datasource {}",
                    sharedStoreLogFileName, context.getFileStorageContainerName(), dbConnectionString, e);
        }
    }

    protected BlobServiceClient getBlobServiceClient() throws Exception {
        if (context.getFileStorageConnectionString() == null) {
            throw new IllegalArgumentException("File storage connection string not set");
        }

        if (blobServiceClient == null) {
            blobServiceClient = new BlobServiceClientBuilder()
                    .connectionString(context.getFileStorageConnectionString()).buildClient();
        }

        return blobServiceClient;
    }

    protected BlobContainerClient getContainerClient() throws Exception {
        final BlobContainerClient containerClient = getBlobServiceClient()
                .getBlobContainerClient(context.getFileStorageContainerName());;
        containerClient.createIfNotExists();
        return containerClient;
    }

    protected AppendBlobClient getSharedStoreLogFile() throws Exception {
        final AppendBlobClient appendBlobClient = getContainerClient()
                .getBlobClient(JDBCLOGS_DIRECTORY + "/" + sharedStoreLogFileName).getAppendBlobClient();
        appendBlobClient.createIfNotExists();
        return appendBlobClient;
    }

    protected BlockBlobClient getZippedLogBlobFile(final String migrationId) throws Exception {
        final String logFileName = getLogFileName(migrationId, true);
        return getContainerClient().getBlobClient(JDBCLOGS_DIRECTORY + "/" + logFileName).getBlockBlobClient();
    }

    private String getLogFileName(final String migrationId, final boolean isZipped) {
        final String filePrefix = isSourceDB ? "source" : "target";
        final String extension = isZipped ? "zip" : "log";
        return migrationId + "-" + filePrefix + "-jdbc-logs." + extension;
    }

    public boolean isSourceDB() {
        return isSourceDB;
    }
}
