/*
 *  Copyright: 2026 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.concurrent;

/**
 * Used to separate database reading and writing operations, after reading data
 * from the DB, the result is put to the pipe and can be used by the database
 * writer later on -> asynchronously
 *
 * @param <T>
 */
public interface DataPipe<T> {
    void requestAbort(Exception e);

    void put(MaybeFinished<T> value) throws Exception;

    MaybeFinished<T> get() throws Exception;

    int size();

    int getWaitersCount();
}
