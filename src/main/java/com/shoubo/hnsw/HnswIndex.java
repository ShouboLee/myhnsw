package com.shoubo.hnsw;

import com.shoubo.Index;
import com.shoubo.Item;
import com.shoubo.listener.ProgressListener;
import com.shoubo.model.DistanceType;
import com.shoubo.model.bo.SearchResultBO;
import com.shoubo.serializer.ObjectSerializer;
import com.shoubo.utils.ArrayBitSet;
import com.shoubo.utils.GenericObjectPool;
import lombok.Data;
import org.eclipse.collections.api.list.primitive.MutableIntList;
import org.eclipse.collections.api.map.primitive.MutableObjectIntMap;
import org.eclipse.collections.api.map.primitive.MutableObjectLongMap;
import org.eclipse.collections.impl.map.mutable.primitive.ObjectIntHashMap;
import org.eclipse.collections.impl.map.mutable.primitive.ObjectLongHashMap;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Author: shoubo
 * Date: 2023/5/27
 * Desc: HnswIndex 这里包含了HNSW算法的主要具体实现
 *
 * @param <TId> Item的唯一标识符的类型
 * @param <TVector> Vector的类型
 * @param <TItem> Item的类型
 * @param <TDistance> 距离的类型
 * @论文链接 <a href="https://arxiv.org/abs/1603.09320">
 *  * Efficient and robust approximate nearest neighbor search using Hierarchical Navigable Small World graphs</a>
 */
public class HnswIndex<TId, TVector, TItem extends Item<TId, TVector>, TDistance>
        implements Index<TId, TVector, TItem, TDistance> {
    /**
     * HNSW索引的版本
     */
    private static final byte VERSION_1 = 0x01;

    /**
     * 序列化版本ID
     */
    private static final long serialVersionUID = 1L;

    /**
     * 用于生成线程名称的原子计数器
     */
    private static final int NO_NODE_ID = -1;

    /**
     * 距离类型选择器
     */
    private DistanceType<TVector, TDistance> distanceType;

    /**
     * 距离比较器
     */
    private Comparator<TDistance> distanceComparator;

    /**
     * 距离比较器，用于比较距离的最大值
     */
    private MaxValueComparator<TDistance> maxValueDistanceComparator;

    private int dimensions;

    private int maxItemCount;

    private int m;

    private int maxM;

    private int maxM0;

    private double levelLambda;

    private int ef;

    private int efConstruction;

    private boolean removeEnabled;

    private int nodeCount;

    private volatile Node<TItem> entryPoint;

    private AtomicReferenceArray<Node<TItem>> nodes;

    private MutableObjectIntMap<TId> lookup;

    private MutableObjectLongMap<TItem> deletedItemVersions;

    private Map<TId, Object> itemLocks;

    private ObjectSerializer<TId> itemIdSerializer;

    private ObjectSerializer<TItem> itemSerializer;

    private ReentrantLock globalLock;

    private GenericObjectPool<ArrayBitSet> visitedBitSetPool;

    private ArrayBitSet excludedCandidates;

    private ExactView exactView;


    private HnswIndex(RefinedBuilder<TId, TVector, TItem, TDistance> builder) {
        this.distanceType = builder.distanceType;
        this.distanceComparator = builder.distanceComparator;
        this.maxValueDistanceComparator = new MaxValueComparator<>(this.distanceComparator);
        this.dimensions = builder.dimensions;
        this.maxItemCount = builder.maxItemCount;

        this.m = builder.m;
        this.maxM = builder.m;
        this.maxM0 = builder.m * 2;
        this.levelLambda = 1 / Math.log(this.m);
        this.ef = builder.ef;
        this.efConstruction = Math.max(builder.efConstruction, m);
        this.removeEnabled = builder.removeEnabled;

        this.nodes = new AtomicReferenceArray<>(maxItemCount);

        this.lookup = new ObjectIntHashMap<>();
        this.deletedItemVersions = new ObjectLongHashMap<>();
        this.itemLocks = new HashMap<>();

        this.itemIdSerializer = builder.itemIdSerializer;
        this.itemSerializer = builder.itemSerializer;

        this.globalLock = new ReentrantLock();

        this.visitedBitSetPool = new GenericObjectPool<>(() -> new ArrayBitSet(this.maxItemCount),
                Runtime.getRuntime().availableProcessors());

        this.excludedCandidates = new ArrayBitSet(maxItemCount);

        this.exactView = new ExactView();
    }

    @Override
    public boolean add(TItem item) {
        return false;
    }

    @Override
    public boolean remove(TId id, long version) {
        return false;
    }

    /**
     * 返回索引中的项数 该方法是线程安全的 但是它的返回值可能不是最新的
     * 因为在返回值之后可能会有新的项被添加到索引中
     * @return 索引中的项数
     */
    @Override
    public int size() {
        globalLock.lock();
        try {
            return lookup.size();
        } finally {
            globalLock.unlock();
        }
    }

    @Override
    public Optional<TItem> get(TId id) {
        globalLock.lock();
        try {
            int nodeId = lookup.getIfAbsent(id, NO_NODE_ID);
            if (nodeId == NO_NODE_ID) {
                return Optional.empty();
            }
            return Optional.ofNullable(nodes.get(nodeId)).map(Node::getItem);
        } finally {
            globalLock.unlock();
        }
    }

    @Override
    public Collection<TItem> items() {
        return null;
    }

    @Override
    public List<SearchResultBO<TItem, TDistance>> findNearest(TVector tVector, int k) {
        return null;
    }

    @Override
    public void save(OutputStream out) throws IOException {

    }

    /**
     * HNSW索引的构造函数 用于创建一个新的HNSW索引 该索引使用默认的参数
     */
    class ExactView implements Index<TId, TVector, TItem, TDistance> {

        /**
         * 序列化版本ID
         */
        private static final long serialVersionUID = 1L;

        /**
         * 新增一个item
         * @param item item
         * @return 是否成功
         */
        @Override
        public boolean add(TItem item) {
            return HnswIndex.this.add(item);
        }

        /**
         * 删除一个item
         * @param id item的id
         * @param version item的版本
         * @return 是否成功
         */
        @Override
        public boolean remove(TId id, long version) {
            return HnswIndex.this.remove(id, version);
        }

        /**
         * 获取item的数量
         * @return item的数量
         */
        @Override
        public int size() {
            return HnswIndex.this.size();
        }

        /** 获取item
         * @param id item的id
         * @return item
         */
        @Override
        public Optional<TItem> get(TId id) {
            return HnswIndex.this.get(id);
        }

        /**
         * 获取所有的item
         * @return 所有的item
         */
        @Override
        public Collection<TItem> items() {
            return HnswIndex.this.items();
        }

        /**
         * 查找最近的向量k个 这是一个精确的方法，它遍历所有的向量。
         * @param vector 向量
         * @param k 数目
         * @return 最近的向量k个
         */
        @Override
        public List<SearchResultBO<TItem, TDistance>> findNearest(TVector vector, int k) {

            // 用于比较距离的比较器
            Comparator<SearchResultBO<TItem, TDistance>> comparator = Comparator
                    .<SearchResultBO<TItem, TDistance>>naturalOrder()
                    .reversed();

            // 用于存储最近的向量k个 优先队列 优先队列的大小为k 优先队列的比较器为comparator
            PriorityQueue<SearchResultBO<TItem, TDistance>> topResults = new PriorityQueue<>(k, comparator);

            // 遍历所有的向量 通过距离比较器比较距离 选出最近的向量k个
            for (int i = 0; i < nodeCount; i++) {
                Node<TItem> node = nodes.get(i);
                if (node == null || node.deleted) {
                    continue;
                }
                TDistance distance = distanceType.distance(node.item.vector(), vector);
                topResults.add(new SearchResultBO<>(distance, node.item, maxValueDistanceComparator));
                if (topResults.size() > k) {
                    topResults.poll();
                }
            }

            // 将优先队列中的结果转换为List
            List<SearchResultBO<TItem, TDistance>> results = new ArrayList<>(topResults.size());

            // 从优先队列中取出结果
            SearchResultBO<TItem, TDistance> result;
            while ((result = topResults.poll()) != null) {
                results.add(result);
            }
            return results;
        }

        /**
         * 保存Index到输出流
         * @param out Index的输出流
         * @throws IOException IO异常
         */
        @Override
        public void save(OutputStream out) throws IOException {
            HnswIndex.this.save(out);
        }

        /**
         * 保存Index到文件
         * @param file Index的文件
         * @throws IOException IO异常
         */
        @Override
        public void save(File file) throws IOException {
            HnswIndex.this.save(file);
        }

        /**
         * 保存Index到路径
         * @param path Index的路径
         * @throws IOException IO异常
         */
        @Override
        public void save(Path path) throws IOException {
            HnswIndex.this.save(path);
        }

        /**
         * 添加所有的item
         * @param items item的集合
         * @throws InterruptedException 中断异常
         */
        @Override
        public void addAll(Collection<TItem> items) throws InterruptedException {
            HnswIndex.this.addAll(items);
        }

        /**
         * 添加所有的item
         * @param items item的集合
         * @param listener 进度监听器
         * @throws InterruptedException 中断异常
         */
        @Override
        public void addAll(Collection<TItem> items, ProgressListener listener) throws InterruptedException {
            HnswIndex.this.addAll(items, listener);
        }

        /**
         * 添加所有的item
         * @param items item的集合
         * @param numThreads 线程数
         * @param listener 进度监听器
         * @throws InterruptedException 中断异常
         */
        public void addAll(Collection<TItem> items, int numThreads, ProgressListener listener, int progressUpdateInterval) throws InterruptedException {
            HnswIndex.this.addAll(items, numThreads, listener, progressUpdateInterval);
        }
    }

    /**
     * 节点
     * @param <TItem> Item的类型
     */
    @Data
    static class Node<TItem> implements Serializable {

        /**
         * 序列化版本ID
         */
        private static final long serialVersionUID = 1L;

        /**
         * 节点的ID
         */
        final int id;

        /**
         * 节点的层级, connections[level] 包含了层级为level的节点的IDs
         */
        final MutableIntList[] connections;

        /**
         * 节点的Item
         */
        volatile TItem item;

        /**
         * 节点是否被删除
         */
        volatile boolean deleted;

        /**
         * 节点的最大层级
         * @return 节点的最大层级
         */
        int maxLevel() {
            return this.connections.length - 1;
        }

    }

    static class MaxValueComparator<TDistance> implements Comparator<TDistance>, Serializable {

        /**
         * 序列化版本ID
         */
        private static final long serialVersionUID = 1L;

        /**
         * 用于比较距离的比较器
         */
        private final Comparator<TDistance> delegate;

        /**
         * 构造方法
         * @param delegate 用于比较距离的比较器
         */
        MaxValueComparator(Comparator<TDistance> delegate) {
            this.delegate = delegate;
        }

        /**
         * 比较两个对象。如果第一个对象大于第二个对象，则返回正整数；如果第一个对象小于第二个对象，则返回负整数；如果两个对象相等，则返回0。
         * @param o1 第一个被比较的对象
         * @param o2 第二个被比较的对象
         * @return 如果第一个对象大于第二个对象，则返回正整数；如果第一个对象小于第二个对象，则返回负整数；如果两个对象相等，则返回0。
         */
        @Override
        public int compare(TDistance o1, TDistance o2) {
            return o1 == null ? o2 == null ? 0 : 1 : o2 == null ? -1 : delegate.compare(o1, o2);
        }
    }

    /**
     * Builder构建器
     * @param <TVector> 向量的类型
     * @param <TDistance> 距离的类型
     */
    public static class Builder<TVector, TDistance> extends BuilderBase<Builder<TVector, TDistance>, TVector, TDistance> {

        /**
         * 构造方法
         *
         * @param dimensions          向量的维度
         * @param distanceType        距离的类型
         * @param tDistanceComparator 用于比较距离的比较器
         * @param maxItemCount        最大的Item数量
         */
        Builder(int dimensions, DistanceType<TVector, TDistance> distanceType, Comparator<TDistance> tDistanceComparator, int maxItemCount) {
            super(dimensions, distanceType, tDistanceComparator, maxItemCount);
        }

        @Override
        Builder<TVector, TDistance> self() {
            return this;
        }

        /**
         * 构建Index Builder
         * @return Index
         */
        public <TId, TItem extends Item<TId, TVector>> RefinedBuilder<TId, TVector, TItem, TDistance> withCustomSerializers(
                ObjectSerializer<TId> itemIdSerializer, ObjectSerializer<TItem> itemSerializer
        ) {
            return new RefinedBuilder<>(
                    dimensions,
                    distanceType,
                    distanceComparator,
                    maxItemCount,
                    m,
                    ef,
                    efConstruction,
                    removeEnabled,
                    itemIdSerializer,
                    itemSerializer);
        }
    }

    /**
     * Builder构建器
     * @param <TId> ID的类型
     * @param <TVector> 向量的类型
     * @param <TItem> Item的类型
     * @param <TDistance> 距离的类型
     */
    public static class RefinedBuilder<TId, TVector, TItem extends Item<TId, TVector>, TDistance>
            extends BuilderBase<RefinedBuilder<TId, TVector, TItem, TDistance>, TVector, TDistance> {


        private ObjectSerializer<TId> itemIdSerializer;

        private ObjectSerializer<TItem> itemSerializer;

        RefinedBuilder(
                int dimensions,
                DistanceType<TVector, TDistance> distanceType,
                Comparator<TDistance> tDistanceComparator,
                int maxItemCount,
                int m,
                int ef,
                int efConstruction,
                boolean removeEnabled,
                ObjectSerializer<TId> itemIdSerializer,
                ObjectSerializer<TItem> itemSerializer
        ) {
            super(dimensions, distanceType, tDistanceComparator, maxItemCount);

            this.m = m;
            this.ef = ef;
            this.efConstruction = efConstruction;
            this.removeEnabled = removeEnabled;

            this.itemIdSerializer = itemIdSerializer;
            this.itemSerializer = itemSerializer;
        }

        @Override
        RefinedBuilder<TId, TVector, TItem, TDistance> self() {
            return this;
        }

        /**
         * 构建Index Builder
         * @return Index
         */
        public RefinedBuilder<TId, TVector, TItem, TDistance> withCustomSerializers (
                ObjectSerializer<TId> itemIdSerializer,
                ObjectSerializer<TItem> itemSerializer
        ) {
            this.itemIdSerializer = itemIdSerializer;
            this.itemSerializer = itemSerializer;

            return this;
        }

        /**
         * 构建Index
         * @return Index
         */
        public HnswIndex<TId, TVector, TItem, TDistance> build() {
            return new HnswIndex<>(
                    this
            );
        }
    }

    /**
     * 构建器基类 (BuilderBase) 是一个抽象类，它实现了 Builder 接口，并且提供了一些默认的实现。
     * 它的子类可以通过继承它来实现自己的构建器。
     * @param <TBuilder> 构建器的类型
     * @param <TVector> 向量的类型
     * @param <TDistance> 距离的类型
     */
    public static abstract class BuilderBase<TBuilder extends BuilderBase<TBuilder, TVector, TDistance>, TVector, TDistance> {

        /**
         * 默认的M值 用于设置每个新元素在构建过程中创建的双向链接数量。
         */
        public static final int DEFAULT_M = 10;

        /**
         * 默认的EF值 用于设置搜索过程中最近邻动态列表的大小。
         */
        public static final int DEFAULT_EF = 10;

        /**
         * 默认的EFConstruction值 用于控制索引的构建时间和索引质量。
         */
        public static final int DEFAULT_EF_CONSTRUCTION = 200;

        /**
         * 默认的removeEnabled值 默认的删除功能启用标志
         */
        public static final boolean DEFAULT_REMOVE_ENABLED = false;

        /**
         * 向量的维度
         */
        int dimensions;

        /**
         * 距离的类型
         */
        DistanceType<TVector, TDistance> distanceType;

        /**
         * 用于比较距离的比较器
         */
        Comparator<TDistance> distanceComparator;

        /**
         * 最大的Item数量
         * */
        int maxItemCount;

        /**
         * 每个新元素在构建过程中创建的双向链接数的参数
         */
        int m = DEFAULT_M;

        /**
         * 用于最近邻搜索的动态列表的大小的参数
         */
        int ef = DEFAULT_EF;

        /**
         * 控制索引构建时间/索引精度的参数
         * */
        int efConstruction = DEFAULT_EF_CONSTRUCTION;

        /**
         * 用于启用实验性的删除操作的标志
         */
        boolean removeEnabled = DEFAULT_REMOVE_ENABLED;

        /**
         * 构造方法
         * @param dimensions 向量的维度
         * @param distanceType 距离的类型
         * @param distanceComparator 用于比较距离的比较器
         * @param maxItemCount 最大的Item数量
         */
        BuilderBase(int dimensions,
                    DistanceType<TVector, TDistance> distanceType,
                    Comparator<TDistance> distanceComparator,
                    int maxItemCount) {
            this.dimensions = dimensions;
            this.distanceType = distanceType;
            this.distanceComparator = distanceComparator;
            this.maxItemCount = maxItemCount;
        }

        /**
         * 获取构建器 这个方法的作用是返回当前的构建器对象本身，即返回一个具体类型为 TBuilder 的构建器对象。
         * 目的是支持方法链式调用（Method Chaining），使构建器的连续调用更加优雅和便捷。
         * @return 构建器
         */
        abstract TBuilder self();

        /**
         * 设置 bi-directional links 的数量
         * 在构建索引时，参数 m 表示每个新元素创建的双向链接数量。合理的取值范围是 2 到 100。
         * 较高的 m 值适用于具有高内在维度和/或高召回率的数据集，而较低的 m 值适用于具有低内在维度和/或低召回率的数据集。
         * 参数 m 还影响算法的内存消耗。
         * 例如，对于维度为 4 的随机向量，最佳的搜索 m 值大约为 6；而对于高维度的数据集（如单词嵌入、良好的人脸描述符），
         * 需要较高的 m 值（如 m = 48、64）以获得高召回率下的最佳性能。一般来说，m 的取值范围在 12 到 48 之间适用于大多数情况。
         * 当修改了 m 值时，也需要相应更新其他参数。然而，可以通过假设 m * efConstruction 为常数来粗略估计 ef 和 efConstruction 参数的取值。
         *
         * @param m 双向 links 的数量
         * @return 构建器
         */
        public TBuilder withM(int m) {
            this.m = m;
            return self();
        }

        /**
         * 设置动态列表中的最近邻居数量
         * ef 参数用于控制最近邻搜索过程中的动态列表的大小。增大 ef 值可以提高搜索结果的准确性，但会导致搜索速度变慢。
         * 根据需求，可以选择适当的 ef 值，使其在近邻数量和数据集大小之间取值。
         *
         * @param ef 动态列表中的最近邻居数量
         * @return 构建器
         */
        public TBuilder withEf(int ef) {
            this.ef = ef;
            return self();
        }

        /**
         * 设置索引构建时的参数 efConstruction。它控制索引的构建时间和索引的质量
         * efConstruction 参数与 ef 具有相同的含义，但它控制了索引的构建时间和索引的精度。
         * 较大的 efConstruction 值会导致更长的构建时间，但索引的质量会更好。
         * 然而，在某个点上，增加 efConstruction 的值并不会进一步提高索引的质量。
         * 一种检查 efConstruction 参数选择是否合适的方法是在 ef = efConstruction
         * 的情况下测量 M 最近邻搜索的召回率：如果召回率低于 0.9，则说明还有改进的空间。
         *
         * @param efConstruction 索引构建时的参数 efConstruction
         * @return 构建器
         */
        public TBuilder withEfConstruction(int efConstruction) {
            this.efConstruction = efConstruction;
            return self();
        }

        /**
         * 启用索引的实验性移除操作（remove）
         * @param removeEnabled 是否允许删除节点
         * @return 构建器
         */
        public TBuilder withRemoveEnabled(boolean removeEnabled) {
            this.removeEnabled = removeEnabled;
            return self();
        }
    }

}
