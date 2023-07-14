/*
 *  Copyright: 2023 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package de.hybris.platform.azure.media;

import de.hybris.platform.core.Registry;
import de.hybris.platform.media.storage.MediaStorageConfigService;
import de.hybris.platform.util.Config;
import org.apache.commons.lang.StringUtils;

public class AzureCloudUtils {
    private static final int MIN_AZURE_MEDIA_FOLDER_QUALIFIER_SIZE = 3;
    private static final int MAX_AZURE_MEDIA_FOLDER_QUALIFIER_SIZE = 63;
    private static final String AZURE_MEDIA_FOLDER_QUALIFIER_REGEX = "[a-z0-9-]+";
    private static final char HYPHEN = '-';
    private static final String DOUBLE_HYPHEN = "--";

    public AzureCloudUtils() {
    }

    public static String computeContainerAddress(MediaStorageConfigService.MediaFolderConfig config) {
        String configuredContainer = config.getParameter("containerAddress");
        String addressSuffix = StringUtils.isNotBlank(configuredContainer)
                ? configuredContainer
                : config.getFolderQualifier();
        String addressPrefix = getTenantPrefix();
        return toValidContainerName(addressPrefix + "-" + addressSuffix);
    }

    private static String toValidContainerName(String name) {
        return name.toLowerCase().replaceAll("[/. !?]", "").replace('_', '-');
    }

    private static String toValidPrefixName(String name) {
        return name.toLowerCase().replaceAll("[/. !?_-]", "");
    }

    private static String getTenantPrefix() {
        // return "sys-" +
        // Registry.getCurrentTenantNoFallback().getTenantID().toLowerCase();
        String defaultPrefix = Registry.getCurrentTenantNoFallback().getTenantID();
        String prefix = toValidPrefixName(Config.getString("db.tableprefix", defaultPrefix));
        return "sys-" + prefix.toLowerCase();
    }

    public static boolean hasValidMediaFolderName(final MediaStorageConfigService.MediaFolderConfig config) {
        final String containerAddress = computeContainerAddress(config);
        return hasValidLength(containerAddress) && hasValidFormat(containerAddress);
    }

    private static boolean hasValidLength(final String folderQualifier) {
        return folderQualifier.length() >= MIN_AZURE_MEDIA_FOLDER_QUALIFIER_SIZE
                && folderQualifier.length() <= MAX_AZURE_MEDIA_FOLDER_QUALIFIER_SIZE;
    }

    private static boolean hasValidFormat(final String folderQualifier) {
        if (!folderQualifier.matches(AZURE_MEDIA_FOLDER_QUALIFIER_REGEX)) {
            return false;
        }

        if (folderQualifier.contains(String.valueOf(HYPHEN))) {
            return hasHyphenValidFormat(folderQualifier);
        }

        return true;
    }

    private static boolean hasHyphenValidFormat(final String folderQualifier) {
        final char firstChar = folderQualifier.charAt(0);
        final char lastChar = folderQualifier.charAt(folderQualifier.length() - 1);
        return !folderQualifier.contains(DOUBLE_HYPHEN) && firstChar != HYPHEN && lastChar != HYPHEN;
    }
}
