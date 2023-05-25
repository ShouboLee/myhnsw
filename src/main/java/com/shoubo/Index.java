package com.shoubo;

import com.shoubo.exception.UncategorizedIndexException;
import com.shoubo.utils.NamedThreadFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Author: shoubo
 * Date: 2023/5/24
 * Desc: 接口定义了K最近邻(KNN)搜索索引的方法
 * 思路是为了提供一种通用的方式来处理K最近邻搜索的操作，并允许不同的实现来适应不同的数据类型和需求。
 *  @param <TId> 数据项（Item）的唯一标识符的类型
 *  @param <TVector> 项的向量类型，用于计算项之间的距离
 *  @param <TItem> 索引中存储的项的类型
 *  @param <TDistance>项之间距离的类型 (可以是任何数值类型: float, double, int, ..)
 */
public interface Index<TId, TVector, TItem extends Item<TId, TVector>, TDistance> extends Serializable {

    /**
     * 默认情况下，在索引固定数量item后，将进度报告给Listener
     */
    int DEFAULT_PROGRESS_UPDATE_INTERVAL = 100_000;

    /**
     * 把新的item添加到索引中，如果具有相同id的item已经存在，则：
     * 如果该index上禁用删除操作，则方法返回false，并且不更新item
     * 如果启用删除，同时item的版本高于当前存储在index中的item的版本，则删除旧item，添加新item
     * @param item
     * @return
     */
    boolean add(TItem item);

    /**
     * 从index中删除item
     * 如果index无删除操作或具有相同id的item存在且具有较高的版本号，则返回false，并且不删除item
     * @param item
     * @param version
     * @return
     */
    boolean remove(TItem item, long version);

    /**
     * 检查index是否包含item
     * @param id
     * @return
     */
    default boolean contains(TId id) {
        return get(id).isPresent();
    }

    /**
     * 将多个item加到index中
     * @param items
     * @throws InterruptedException
     */
    default void addAll(Collection<TItem> items) throws InterruptedException {
        addAll(items, NullProgressListener.INSTANCE);
    }

    /**
     * 将多个item加到index中，每添加一个item都传入报告给listener
     * @param items
     * @param listener
     * @throws InterruptedException
     */
    default void addAll(Collection<TItem> items, ProgressListener listener) throws InterruptedException {
        addAll(items, Runtime.getRuntime().availableProcessors(), listener, DEFAULT_PROGRESS_UPDATE_INTERVAL);
    }

    /**
     * 将多个item添加到index中。每添加 progressUpdateInterval 个元素时，会将进度报告给传入的 listener。
     *      *
     * @param items
     * @param numThreads
     * @param listener
     * @param progressUpdateInterval
     * @throws InterruptedException
     */
    default void addAll(Collection<TItem> items, int numThreads, ProgressListener listener, int progressUpdateInterval)
            throws InterruptedException {
        ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(numThreads, numThreads, 60L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(),
                new NamedThreadFactory("index-%d"));
        threadPoolExecutor.allowCoreThreadTimeOut(true);

        int numItems = items.size();

        AtomicInteger workDone = new AtomicInteger();

        try {
            LinkedBlockingQueue<TItem> blockingQueue = new LinkedBlockingQueue<>(items);
            List<Future<?>> futures = new ArrayList<>();

            for (int threadId = 0; threadId < numThreads; threadId++) {
                futures.add(threadPoolExecutor.submit(() -> {
                    TItem item;
                    while ((item = blockingQueue.poll()) != null) {
                        add(item);

                        int done = workDone.incrementAndGet();
                        if (done % progressUpdateInterval == 0 || numItems == done) {
                            listener.updateProgress(done, items.size());
                        }
                    }
                }));
            }

            for (Future<?> future : futures) {
                try {
                    future.get();
                } catch (ExecutionException e) {
                    throw new UncategorizedIndexException("某个线程中抛出异常.", e.getCause());
                }
            }
        } finally {
            threadPoolExecutor.shutdown();
        }
    }


    /**
    * 根据item的id返回该item
    * 由于可能为空此处用Optional。如果找到了与给定标识符相匹配的项，则将该项包装在 Optional 对象中并返回。
    * 如果找不到匹配的项，则返回一个空的 Optional 对象。
    *
    */
    Optional<TItem> get(TId id);
}
