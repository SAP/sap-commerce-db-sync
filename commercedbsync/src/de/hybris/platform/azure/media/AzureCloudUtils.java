/*
 *  Copyright: 2022 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package de.hybris.platform.azure.media;

import de.hybris.platform.core.Registry;
import de.hybris.platform.media.storage.MediaStorageConfigService.MediaFolderConfig;
import de.hybris.platform.util.Config;
import org.apache.commons.lang.StringUtils;

public class AzureCloudUtils {
    public AzureCloudUtils() {
    }

    public static String computeContainerAddress(MediaFolderConfig config) {
        String configuredContainer = config.getParameter("containerAddress");
        String addressSuffix = StringUtils.isNotBlank(configuredContainer) ? configuredContainer : config.getFolderQualifier();
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
        //return "sys-" + Registry.getCurrentTenantNoFallback().getTenantID().toLowerCase();
        String defaultPrefix = Registry.getCurrentTenantNoFallback().getTenantID();
        String prefix = toValidPrefixName(Config.getString("db.tableprefix", defaultPrefix));
        return "sys-" + prefix.toLowerCase();
    }
}
