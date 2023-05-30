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
     * 无效节点的标识符
     */
    private static final int NO_NODE_ID = -1;

    /**
     * 距离类型选择器 用于计算向量之间的距离
     */
    private DistanceType<TVector, TDistance> distanceType;

    /**
     * 距离比较器 用于比较两个距离的大小
     */
    private Comparator<TDistance> distanceComparator;

    /**
     * 距离比较器 用于比较距离是否超过最大值
     * 在HNSW算法中，需要判断距离是否超过某个阈值（通常是最大距离），以确定是否需要进行进一步的搜索
     */
    private MaxValueComparator<TDistance> maxValueDistanceComparator;

    /**
     * 向量的维度
     */
    private int dimensions;

    /**
     * Index中每个节点允许的最大item数。
     * HNSW算法将数据点存储在Index的节点中 定义了每个节点能够容纳的最大数据点数目。
     */
    private int maxItemCount;

    /**
     * 每个节点中的连接数
     * HNSW算法中的每个节点都与其他节点相连，m表示每个节点在构建时要建立的连接数。
     * 较大的m值可以提供更好的搜索准确性，但会增加索引构建和搜索的时间复杂度。
     */
    private int m;

    /**
     * 每个节点中的最大连接数。
     * 在HNSW算法中，每个节点的连接数可能会随着层级的增加而增加。
     * 但最大连接数受限于maxM的值。即每个节点在构建时可以建立的最大连接数不能超过maxM。
     */
    private int maxM;

    /**
     * 入口点节点的最大连接数
     * 入口点是HNSW索引的起始节点，maxM0定义了入口点节点在构建时可以建立的最大连接数。
     * 这个值通常会与maxM有所不同，以允许更灵活的控制入口点的连接数量。
     */
    private int maxM0;

    /**
     * 层级调整参数
     * 在HNSW算法中，节点之间的连接构成了多个层级。levelLambda用于调整层级的数量。
     * 较大的levelLambda值会增加层级的数量，从而提高搜索的准确性，但也会增加存储和搜索的开销。
     */
    private double levelLambda;

    /**
     * 搜索时的探索因子
     * 在HNSW算法中，搜索过程中需要探索邻近节点以查找更近的数据点。ef控制了搜索时访问的节点数量。
     * 较大的ef值会增加搜索的准确性，但也会增加搜索的时间复杂度。
     */
    private int ef;

    /**
     * 构建时的探索因子
     * 在HNSW算法的索引构建过程中，也需要进行搜索以确定每个节点的邻近节点。
     * efConstruction定义了在构建过程中用于搜索邻近节点的探索因子。
     */
    private int efConstruction;

    /**
     * 是否启用删除功能的标志
     * HNSW算法支持删除操作，即从索引中移除特定的数据点。removeEnabled标志用于控制是否启用删除功能。
     */
    private boolean removeEnabled;

    /**
     * 当前索引中的节点数量
     * nodeCount表示当前索引中有效节点的数量，用于跟踪索引的大小和状态。
     */
    private int nodeCount;

    /**
     * 入口点节点
     * 入口点是HNSW索引的起始节点，entryPoint表示当前索引中的入口点节点。
     */
    private volatile Node<TItem> entryPoint;

    /**
     * 节点数组
     * nodes是一个存储节点的数组，每个节点包含了数据点以及与其他节点的连接信息。
     */
    private AtomicReferenceArray<Node<TItem>> nodes;

    /**
     * 数据点标识符到节点索引的映射
     * lookup是一个映射结构，用于快速查找数据点标识符对应的节点索引。
     */
    private MutableObjectIntMap<TId> lookup;

    /**
     * 已删除数据点的版本记录
     * deletedItemVersions是一个映射结构，用于记录已删除数据点的版本信息。
     */
    private MutableObjectLongMap<TId> deletedItemVersions;

    /**
     * 数据点的锁映射
     * locks是一个映射结构，用于在多线程环境中对数据点进行加锁，以确保并发访问的正确性。
     */
    private Map<TId, Object> itemLocks;

    /**
     * 数据点标识符的序列化器
     * itemIdSerializer用于将数据点的标识符进行序列化和反序列化操作，以便在存储和传输过程中能够进行有效的数据交换和持久化。
     */
    private ObjectSerializer<TId> itemIdSerializer;

    /**
     * 数据点的序列化器
     * itemSerializer用于将数据点进行序列化和反序列化操作，以便在存储和传输过程中能够进行有效的数据交换和持久化。
     */
    private ObjectSerializer<TItem> itemSerializer;

    /**
     * 全局锁
     * globalLock是一个可重入锁（ReentrantLock），用于保护对索引数据的并发访问。它可以在需要对整个索引进行原子操作或保护共享资源时使用。
     */
    private ReentrantLock globalLock;

    /**
     * 已访问位集合对象池
     * 搜索过程会使用位集合来记录已经访问过的节点，visitedBitSetPool是一个对象池，用于缓存和重用位集合对象，以提高搜索的效率。
     */
    private GenericObjectPool<ArrayBitSet> visitedBitSetPool;

    /**
     * 排除候选集合
     * 在搜索过程中，可以排除某些候选节点以减少搜索空间。excludedCandidates是一个位集合，用于存储要排除的候选节点。
     */
    private ArrayBitSet excludedCandidates;

    /**
     * 精确视图
     * exactView是HNSW索引的一部分，用于提供精确的近邻搜索功能。
     */
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
        if (!removeEnabled) {
            return false;
        }

        globalLock.lock();

        try {
            int internalNodeId = lookup.getIfAbsent(id, NO_NODE_ID);
            if (internalNodeId == NO_NODE_ID) {
                return false;
            }

            Node<TItem> node = nodes.get(internalNodeId);

            if (node == null) {
                return false;
            }

            if (node.getItem().version() > version) {
                return false;
            }

            node.deleted = true;

            lookup.remove(id);

            deletedItemVersions.put(id, version);

            return true;
        } finally {
            globalLock.unlock();
        }
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

    /**
     * 返回索引中的所有项 该方法是线程安全的 但是它的返回值可能不是最新的
     * 因为在返回值之后可能会有新的项被添加到索引中
     * @return 索引中的所有项
     */
    @Override
    public Collection<TItem> items() {
        globalLock.lock();
        try {
            List<TItem> results = new ArrayList<>(size());

            Iterator<TItem> iterator = new ItemIterator();

            while (iterator.hasNext()) {
                results.add(iterator.next());
            }

            return results;
        } finally {
            globalLock.unlock();
        }
    }

    @Override
    public List<SearchResultBO<TItem, TDistance>> findNearest(TVector destination, int k) {
        if (entryPoint == null) {
            return Collections.emptyList();
        }

        Node<TItem> entryPointCopy = entryPoint;

        Node<TItem> curObj = entryPointCopy;

        TDistance curDist = distanceType.distance(destination, curObj.getItem().vector());

        for (int activeLevel = entryPointCopy.maxLevel(); activeLevel > 0; activeLevel--) {
            boolean changed = true;

            while (changed) {
                changed = false;

                synchronized (curObj) {
                    MutableIntList candidateConnections = curObj.connections[activeLevel];

                    for (int i = 0; i < candidateConnections.size(); i++) {

                        int candidateId = candidateConnections.get(i);

                        TDistance candidateDist = distanceType.distance(
                                destination,
                                nodes.get(candidateId).getItem().vector()
                        );
                        if (lt(candidateDist, curDist)) {
                            curObj = nodes.get(candidateId);
                            curDist = candidateDist;
                            changed = true;
                        }
                    }
                }
            }
        }

        PriorityQueue<NodeIdAndDistance<TDistance>> topCandidates = searchBaseLayer(
                curObj,
                destination,
                Math.max(ef, k),
                0
        );

        while (topCandidates.size() > k) {
            topCandidates.poll();
        }

        List<SearchResultBO<TItem, TDistance>> results = new ArrayList<>(topCandidates.size());
        while (!topCandidates.isEmpty()) {
            NodeIdAndDistance<TDistance> pair = topCandidates.poll();
            results.add(0, new SearchResultBO<>(pair.distance, nodes.get(pair.nodeId).getItem(), maxValueDistanceComparator));
        }

        return results;
    }

    @Override
    public void save(OutputStream out) throws IOException {

    }

    /**
     * // TODO
     * @param entryPointNode
     * @param destination
     * @param k
     * @param layer
     * @return
     */
    private PriorityQueue<NodeIdAndDistance<TDistance>> searchBaseLayer(
            Node<TItem> entryPointNode,
            TVector destination,
            int k,
            int layer
    ) {
        ArrayBitSet visitedBitSet = visitedBitSetPool.borrowObject();

        try {
            PriorityQueue<NodeIdAndDistance<TDistance>> topCandidates =
                    new PriorityQueue<>(Comparator.<NodeIdAndDistance<TDistance>>naturalOrder().reversed());
            PriorityQueue<NodeIdAndDistance<TDistance>> candidateSet = new PriorityQueue<>();

            TDistance lowerBound;

            if (!entryPointNode.deleted) {
                TDistance distance = distanceType.distance(destination, entryPointNode.getItem().vector());
                NodeIdAndDistance<TDistance> pair = new NodeIdAndDistance<>(entryPointNode.id, distance, maxValueDistanceComparator);

                topCandidates.add(pair);
                lowerBound = distance;
                candidateSet.add(pair);
            } else {
                lowerBound = MaxValueComparator.maxValue();
                NodeIdAndDistance<TDistance> pair = new NodeIdAndDistance<>(entryPointNode.id, lowerBound, maxValueDistanceComparator);
                candidateSet.add(pair);
            }

            visitedBitSet.add(entryPointNode.id);

            while (!candidateSet.isEmpty()) {
                NodeIdAndDistance<TDistance> currentPair = candidateSet.poll();

                if (gt(currentPair.distance, lowerBound)) {
                    break;
                }

                Node<TItem> node = nodes.get(currentPair.nodeId);

                synchronized (node) {

                    MutableIntList candidates = node.connections[layer];

                    for (int i = 0; i < candidates.size(); i++) {

                        int candidateId = candidates.get(i);

                        if (!visitedBitSet.contains(candidateId)) {

                            visitedBitSet.add(candidateId);

                            Node<TItem> candidateNode = nodes.get(candidateId);

                            TDistance candidateDistance = distanceType.distance(destination, candidateNode.getItem().vector());

                            if (topCandidates.size() < k || gt(lowerBound, candidateDistance)) {

                                NodeIdAndDistance<TDistance> candidatePair = new NodeIdAndDistance<>(candidateId, candidateDistance, maxValueDistanceComparator);

                                candidateSet.add(candidatePair);

                                if (!candidateNode.deleted) {
                                    topCandidates.add(candidatePair);
                                }

                                if (topCandidates.size() > k) {
                                    topCandidates.poll();
                                }

                                if (!topCandidates.isEmpty()) {
                                    lowerBound = topCandidates.peek().distance;
                                }
                            }
                        }
                    }
                }
            }
            return topCandidates;
        } finally {
            visitedBitSet.clear();
            visitedBitSetPool.returnObject(visitedBitSet);
        }
    }

    /**
     * 迭代器的实现 用于遍历Index中的所有项
     */
    class ItemIterator implements Iterator<TItem> {

        /**
         * 当前Index
         */
        private int index;

        /**
         * 已经完成的项数
         */
        private int done;

        /**
         * hasNext()方法的实现
         * @return 是否还有下一个元素
         */
        @Override
        public boolean hasNext() {
            return done < HnswIndex.this.size();
        }

        /**
         * next()方法的实现
         * @return 下一个元素
         */
        @Override
        public TItem next() {
            Node<TItem> node;

            do {
                node = HnswIndex.this.nodes.get(index++);
            } while (node == null || node.deleted);

            done++;

            return node.item;
        }
    }

    /**
     * 用于比较两个距离的比较器 小于
     *
     * @param a 距离a
     * @param b 距离b
     * @return a和b的比较结果
     */
    private boolean lt(TDistance a, TDistance b) {
        return maxValueDistanceComparator.compare(a, b) < 0;
    }

    /**
     * 用于比较两个距离的比较器 大于
     *
     * @param a 距离a
     * @param b 距离b
     * @return a和b的比较结果
     */
    private boolean gt(TDistance a, TDistance b) {
        return maxValueDistanceComparator.compare(a, b) > 0;
    }

    static class NodeIdAndDistance<TDistance> implements Comparable<NodeIdAndDistance<TDistance>> {

        final int nodeId;

        final TDistance distance;

        final Comparator<TDistance> distanceComparator;

        NodeIdAndDistance(int nodeId, TDistance distance, Comparator<TDistance> distanceComparator) {
            this.nodeId = nodeId;
            this.distance = distance;
            this.distanceComparator = distanceComparator;
        }

        @Override
        public int compareTo(NodeIdAndDistance<TDistance> o) {
            return distanceComparator.compare(distance, o.distance);
        }
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

        static <TDistance> TDistance maxValue() {
            return null;
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
