/*
 *  Copyright: 2022 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.service.impl;

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.NameValidator;
import com.microsoft.azure.storage.blob.CloudBlob;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import com.microsoft.azure.storage.blob.ListBlobItem;
import com.sap.cx.boosters.commercedbsync.service.DatabaseMigrationReportStorageService;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import com.sap.cx.boosters.commercedbsync.context.MigrationContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

public class BlobDatabaseMigrationReportStorageService implements DatabaseMigrationReportStorageService {

    private static final Logger LOG = LoggerFactory.getLogger(BlobDatabaseMigrationReportStorageService.class.getName());

    private static final String ROOT_CONTAINER = "migration";

    private CloudBlobClient cloudBlobClient;

    private MigrationContext migrationContext;

    protected void init() throws Exception {
        CloudStorageAccount account = CloudStorageAccount.parse(migrationContext.getMigrationReportConnectionString());
        this.cloudBlobClient = account.createCloudBlobClient();
    }

    @Override
    public void store(String fileName, InputStream inputStream) throws Exception {
        String path = fileName;
        if (inputStream != null) {
            CloudBlockBlob blob = getContainer(ROOT_CONTAINER, true).getBlockBlobReference(path);
            byte[] bytes = IOUtils.toByteArray(inputStream);
            ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
            blob.upload(bis, bytes.length);
            bis.close();
            LOG.info("File {} written to blob storage at {}/{}", path, ROOT_CONTAINER, path);
        } else {
            throw new IllegalArgumentException(String.format("Input Stream is null for root '%s' and path '%s'", ROOT_CONTAINER, path));
        }
    }

    protected CloudBlobContainer getContainer(String name, boolean createIfNotExists) throws Exception {
        CloudBlobContainer containerReference = getCloudBlobClient().getContainerReference(name);
        if (createIfNotExists) {
            containerReference.createIfNotExists();
        }
        return containerReference;
    }

    public List<CloudBlockBlob> listAllReports() throws Exception {
        getCloudBlobClient();
        Iterable<ListBlobItem> migrationBlobs = cloudBlobClient.getContainerReference(ROOT_CONTAINER).listBlobs();
        List<CloudBlockBlob> result = new ArrayList<>();
        migrationBlobs.forEach(blob -> {
            result.add((CloudBlockBlob) blob);
        });
        return result;
    }

    public byte[] getReport(String reportId) throws Exception {
        checkReportIdValid(reportId);
        CloudBlob blob = cloudBlobClient.getContainerReference(ROOT_CONTAINER).getBlobReferenceFromServer(reportId);
        byte[] output = new byte[blob.getStreamWriteSizeInBytes()];
        blob.downloadToByteArray(output, 0);
        return output;
    }

    private void checkReportIdValid(String reportId) {
        NameValidator.validateFileName(reportId);
        if (StringUtils.contains(reportId, "/")) {
            throw new IllegalArgumentException("Invalid report id provided");
        }
        if (!StringUtils.endsWith(reportId, ".json") && !StringUtils.endsWith(reportId, ".sql")) {
            throw new IllegalArgumentException("Invalid file name ending provided");
        }
    }

    protected CloudBlobClient getCloudBlobClient() throws Exception {
        if (cloudBlobClient == null) {
            init();
        }
        return cloudBlobClient;
    }

    @Override
    public boolean validateConnection() {
        try {
            getCloudBlobClient().listContainers();
        } catch (Exception e) {
            return false;
        }
        return true;
    }

    public void setMigrationContext(MigrationContext migrationContext) {
        this.migrationContext = migrationContext;
    }
}
