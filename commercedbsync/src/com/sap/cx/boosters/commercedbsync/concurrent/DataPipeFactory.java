/*
 *  Copyright: 2023 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.concurrent;

import com.sap.cx.boosters.commercedbsync.context.CopyContext;

import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public interface DataPipeFactory<T> {
    DataPipe<T> create(CopyContext context, CopyContext.DataCopyItem item) throws Exception;
}
