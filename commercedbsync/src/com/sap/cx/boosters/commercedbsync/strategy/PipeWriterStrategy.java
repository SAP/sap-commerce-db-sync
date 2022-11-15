/*
 *  Copyright: 2022 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.strategy;

import com.sap.cx.boosters.commercedbsync.concurrent.DataPipe;
import com.sap.cx.boosters.commercedbsync.context.CopyContext;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Main Strategy to Write Data to a target Database
 *
 * @param <T>
 */
@ThreadSafe
public interface PipeWriterStrategy<T> {
    /**
     * Performs the actual copying of Data Items
     *
     * @param context
     * @param pipe
     * @param item
     * @throws Exception
     */
    void write(CopyContext context, DataPipe<T> pipe, CopyContext.DataCopyItem item) throws Exception;
}
