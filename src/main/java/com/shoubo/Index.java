package com.shoubo;

import java.io.Serializable;
import java.util.Collection;
import java.util.Optional;

/**
 * Author: shoubo
 * Date: 2023/5/24
 * Desc: K-nearest neighbors search index.
 *  @param <TId> Type of the external identifier of an item
 *  @param <TVector> Type of the vector to perform distance calculation on
 *  @param <TItem> Type of items stored in the index
 *  @param <TDistance> Type of distance between items (expect any numeric type: float, double, int, ..)
 */
public interface Index<TId, TVector, TItem extends Item<TId, TVector>, TDistance> extends Serializable {

    int DEFAULT_PROGRESS_UPDATE_INTERVAL = 100_000;

    boolean add(TItem item);

    boolean remove(TItem item, long version);

    default boolean contains(TId id) {
        return get(id).isPresent();
    }

    default void addAll(Collection<TItem> items) throws InterruptedException {
        addAll(items, NullProgressListener.INSTANCE);
    }

    default void addAll(Collection<TItem> items, ProgressListener listener) throws InterruptedException {
        addAll(items, Runtime.getRuntime().availableProcessors(), listener, DEFAULT_PROGRESS_UPDATE_INTERVAL);
    }

    default void addAll(Collection<TItem> items, int numThreads, ProgressListener listener, int progressUpdateInterval)
        throws InterruptedException {

    }


    /*
    * 根据item的id返回该item
    * 由于可能为空此处用Optional。如果找到了与给定标识符相匹配的项，则将该项包装在 Optional 对象中并返回。
    * 如果找不到匹配的项，则返回一个空的 Optional 对象。
    *
    * */
    Optional<TItem> get(TId id);
}
