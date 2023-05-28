package com.shoubo;

import com.shoubo.exception.UncategorizedIndexException;
import com.shoubo.listener.NullProgressListener;
import com.shoubo.listener.ProgressListener;
import com.shoubo.model.bo.SearchResultBO;
import com.shoubo.utils.NamedThreadFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

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
     * 进度报告频率：默认情况下，在索引固定数量item后，将进度报告给Listener
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
     * @param id item的id
     * @param version item的版本
     * @return 如果item被删除，则返回true，否则返回false
     */
    boolean remove(TId id, long version);

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
     * 使用线程池来实现并行添加项到索引，并提供进度报告
     * @param items items
     * @param numThreads numThreads
     * @param listener listener
     * @param progressUpdateInterval progressUpdateInterval
     * @throws InterruptedException 异常
     */
    default void addAll(Collection<TItem> items, int numThreads, ProgressListener listener, int progressUpdateInterval)
            throws InterruptedException {
        // 创建线程池，使用无界阻塞队列作为任务队列，采用自定义线程工厂
        ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(numThreads, numThreads, 60L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(),
                new NamedThreadFactory("index-%d"));
        // 允许核心线程超时
        threadPoolExecutor.allowCoreThreadTimeOut(true);

        int numItems = items.size();

        AtomicInteger workDone = new AtomicInteger();

        try {
            // 创建一个 LinkedBlockingQueue，将传入的项集合作为初始队列，每个线程从队列中获取项进行添加操作
            LinkedBlockingQueue<TItem> itemsQueue = new LinkedBlockingQueue<>(items);
            // 保存提交给线程池的任务的返回结果
            List<Future<?>> futures = new ArrayList<>();

            for (int threadId = 0; threadId < numThreads; threadId++) {
                // 使用 Lambda 表达式来定义了一个匿名的 Runnable 对象，作为线程池中线程的执行任务。
                futures.add(threadPoolExecutor.submit(() -> {
                    TItem item;
                    while ((item = itemsQueue.poll()) != null) {
                        add(item);
                        int done = workDone.incrementAndGet();
                        // 控制进度更新
                        if (done % progressUpdateInterval == 0 || numItems == done) {
                            listener.updateProgress(done, items.size());
                        }
                    }
                }));
            }
            // 等待所有任务执行完成，使用 Future.get() 获取任务的结果
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
     * 获得Index大小
     * @return Index大小
     */
    int size();

    /**
    * 根据item的id返回该item
    * 由于可能为空此处用Optional。如果找到了与给定标识符相匹配的项，则将该项包装在 Optional 对象中并返回。
    * 如果找不到匹配的项，则返回一个空的 Optional 对象。
    *
    */
    Optional<TItem> get(TId id);

    /**
     * 获得Index的所有items 集合形式
     * @return Index包含的所有items
     */
    Collection<TItem> items();

    /**
     * 找到距离传入向量vector最近的k个item
     * @param vector 向量
     * @param k 数目
     * @return SearchResultBO列表
     */
    List<SearchResultBO<TItem, TDistance>> findNearest(TVector vector, int k);

    /**
     * 查找与指定ID对应的item最接近的k个邻居items
     * @param id ID
     * @param k 数目
     * @return items列表
     */
    default List<SearchResultBO<TItem, TDistance>> findNeighbors(TId id, int k) {
        return get(id).map(item -> findNearest(item.vector(), k+1).stream() // 将k个邻居item转为stream
                .filter(result -> !result.getItem().id().equals(id))    // 排除自身
                .limit(k)   // 限制k个
                .collect(Collectors.toList())) // 将结果保留到List中
                .orElse(Collections.emptyList());
    }

    /**
     * 将Index保存为输出流
     * 注意：保存操作不是线程安全的，不要在保存的同时修改Index
     * @param out Index的输出流
     * @throws IOException IO异常
     */
    void save(OutputStream out) throws IOException;

    /**
     * 将Index保存为文件
     * 注意：保存操作不是线程安全的，期间不能修改Index
     * @param file Index的保存文件
     * @throws IOException IO异常
     */
    default void save(File file) throws IOException {
        save(Files.newOutputStream(file.toPath()));
    }

    /**
     * 将Index保存为指定的文件路径
     * 注意：保存操作非线程安全，期间不能修改Index
     * @param path 指定文件路径
     * @throws IOException IO异常
     */
    default void save(Path path) throws IOException {
        save(Files.newOutputStream(path));
    }

}
