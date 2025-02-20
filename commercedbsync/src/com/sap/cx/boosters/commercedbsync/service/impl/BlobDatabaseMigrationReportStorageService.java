/*
 *  Copyright: 2023 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.service.impl;

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.specialized.BlockBlobClient;
import com.sap.cx.boosters.commercedbsync.service.DatabaseMigrationReportStorageService;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import com.sap.cx.boosters.commercedbsync.context.MigrationContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

public class BlobDatabaseMigrationReportStorageService implements DatabaseMigrationReportStorageService {

    private static final Logger LOG = LoggerFactory
            .getLogger(BlobDatabaseMigrationReportStorageService.class.getName());

    private BlobServiceClient blobServiceClient;

    private MigrationContext migrationContext;

    protected void init() throws Exception {
        LOG.info("Connecting to blob storage {}", migrationContext.getFileStorageConnectionString());
        this.blobServiceClient = new BlobServiceClientBuilder()
                .connectionString(migrationContext.getFileStorageConnectionString()).buildClient();
    }

    @Override
    public void store(String fileName, InputStream inputStream) throws Exception {
        final String containerName = migrationContext.getFileStorageContainerName();
        if (inputStream != null) {
            final BlockBlobClient blobClient = getContainerClient(containerName, true).getBlobClient(fileName)
                    .getBlockBlobClient();
            byte[] bytes = IOUtils.toByteArray(inputStream);
            final ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
            blobClient.upload(bis, bytes.length);
            bis.close();
            LOG.info("File {} written to blob storage at {}/{}", fileName, containerName, fileName);
        } else {
            throw new IllegalArgumentException(
                    String.format("Input Stream is null for root '%s' and path '%s'", containerName, fileName));
        }
    }

    protected BlobContainerClient getContainerClient(String name, boolean createIfNotExists) throws Exception {
        final BlobContainerClient containerClient = getBlobServiceClient().getBlobContainerClient(name);
        if (createIfNotExists) {
            containerClient.createIfNotExists();
        }
        return containerClient;
    }

    public List<BlobClient> listAllReports() throws Exception {
        final String containerName = migrationContext.getFileStorageContainerName();
        final List<BlobClient> result = new ArrayList<>();
        final BlobContainerClient containerClient = getContainerClient(containerName, true);
        containerClient.listBlobs().forEach(blob -> {
            if (!blob.isPrefix() && blob.getName().endsWith(".json")) {
                result.add(containerClient.getBlobClient(blob.getName()));
            }
        });
        return result;
    }

    public byte[] getReport(String reportId) throws Exception {
        checkReportIdValid(reportId);
        final String containerName = migrationContext.getFileStorageContainerName();
        final BlobClient blobClient = getContainerClient(containerName, false).getBlobClient(reportId);
        final ByteArrayOutputStream result = new ByteArrayOutputStream();
        blobClient.downloadStream(result);
        return result.toByteArray();
    }

    private void checkReportIdValid(String reportId) {
        if (StringUtils.contains(reportId, "/")) {
            throw new IllegalArgumentException("Invalid report id provided");
        }
        if (!StringUtils.endsWith(reportId, ".json") && !StringUtils.endsWith(reportId, ".sql")) {
            throw new IllegalArgumentException("Invalid file name ending provided");
        }
    }

    protected BlobServiceClient getBlobServiceClient() throws Exception {
        if (blobServiceClient == null) {
            init();
        }
        return blobServiceClient;
    }

    @Override
    public boolean validateConnection() {
        try {
            getBlobServiceClient().listBlobContainers();
        } catch (Exception e) {
            return false;
        }
        return true;
    }

    public void setMigrationContext(MigrationContext migrationContext) {
        this.migrationContext = migrationContext;
    }
}
