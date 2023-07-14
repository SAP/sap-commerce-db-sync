/*
 *  Copyright: 2023 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.concurrent;

/**
 * MaybeFinished keeps track status of the data set that is currently being
 * processed -> if all is ok, then status will be done, if theres an exception,
 * it will be poison
 *
 * @param <T>
 */
public final class MaybeFinished<T> {
    private final T value;
    private final boolean done;
    private final boolean poison;

    private MaybeFinished(T value, boolean done, boolean poison) {
        this.value = value;
        this.done = done;
        this.poison = poison;
    }

    public static <T> MaybeFinished<T> of(T value) {
        return new MaybeFinished<>(value, false, false);
    }

    public static <T> MaybeFinished<T> finished(T value) {
        return new MaybeFinished<>(value, true, false);
    }

    public static <T> MaybeFinished<T> poison() {
        return new MaybeFinished<>(null, true, true);
    }

    public T getValue() {
        return value;
    }

    public boolean isDone() {
        return done;
    }

    public boolean isPoison() {
        return poison;
    }
}
