/*
 *  Copyright: 2022 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.service;

import com.sap.cx.boosters.commercedbsync.repository.DataRepository;

public interface DatabaseMigrationSynonymService {

    /**
     * CCv2 Workaround: ccv2 builder does not support prefixes yet.
     * creating synonym on ydeployments -> prefix_yeployments
     * creating synonym on attributedescriptors -> prefix_attributedescriptors.
     *
     * @param repository
     * @param prefix
     * @throws Exception
     */
    void recreateSynonyms(DataRepository repository, String prefix) throws Exception;
}
