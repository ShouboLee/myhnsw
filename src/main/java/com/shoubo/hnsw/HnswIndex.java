package com.shoubo.hnsw;

import com.shoubo.Index;
import com.shoubo.Item;
import com.shoubo.exception.SizeLimitExceededException;
import com.shoubo.listener.ProgressListener;
import com.shoubo.model.DistanceType;
import com.shoubo.model.bo.SearchResultBO;
import com.shoubo.serializer.ObjectSerializer;
import com.shoubo.utils.ArrayBitSet;
import com.shoubo.utils.ClassLoaderObjectInputStream;
import com.shoubo.utils.GenericObjectPool;
import com.shoubo.utils.Murmur3;
import lombok.Data;
import org.eclipse.collections.api.list.primitive.MutableIntList;
import org.eclipse.collections.api.map.primitive.MutableObjectIntMap;
import org.eclipse.collections.api.map.primitive.MutableObjectLongMap;
import org.eclipse.collections.api.tuple.primitive.ObjectIntPair;
import org.eclipse.collections.api.tuple.primitive.ObjectLongPair;
import org.eclipse.collections.impl.list.mutable.primitive.IntArrayList;
import org.eclipse.collections.impl.map.mutable.primitive.ObjectIntHashMap;
import org.eclipse.collections.impl.map.mutable.primitive.ObjectLongHashMap;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Author: shoubo
 * Date: 2023/5/27
 * Desc: HnswIndex 这里包含了HNSW算法的主要具体实现
 *
 * @param <TId>       Item的唯一标识符的类型
 * @param <TVector>   Vector的类型
 * @param <TItem>     Item的类型
 * @param <TDistance> 距离的类型
 * @论文链接 <a href="https://arxiv.org/abs/1603.09320">
 * * Efficient and robust approximate nearest neighbor search using Hierarchical Navigable Small World graphs</a>
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
     * 在HNSW算法中，支持删除操作，即从索引中移除特定的数据点。
     * 为了确保删除操作的正确性，需要记录每个数据点的版本信息
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

    /**
     * 向索引中添加一个新的数据点
     *
     * @param item 要添加的数据点
     * @return 如果成功添加，则返回true；如果数据点已存在，则返回false
     */
    @Override
    public boolean add(TItem item) {
        // 检查数据点的维度是否正确
        if (item.dimensions() != dimensions) {
            throw new IllegalArgumentException("Item维度不正确, item维度: " + item.dimensions() + " Index维度: " + dimensions);
        }

        // 为新节点分配随机层级
        int randomLevel = assignLevel(item.id(), this.levelLambda);

        // 为每个层级创建一个连接列表
        IntArrayList[] connections = new IntArrayList[randomLevel + 1];

        for (int level = 0; level <= randomLevel; level++) {
            int levelM = randomLevel == 0 ? maxM0 : maxM;
            connections[level] = new IntArrayList(levelM);
        }

        // 获取全局锁
        globalLock.lock();

        try {
            // 检查数据点是否已存在于索引中
            int existingNodeId = lookup.getIfAbsent(item.id(), NO_NODE_ID);

            if (existingNodeId != NO_NODE_ID) {
                // 如果数据点已存在，则根据removeEnabled标志决定是否更新数据点
                if (!removeEnabled) {
                    return false;
                }

                Node<TItem> node = nodes.get(existingNodeId);

                // 如果数据点的版本号小于等于已存在节点的版本号，则不更新数据点
                if (item.version() < node.getItem().version()) {
                    return false;
                }

                // 如果数据点的向量与已存在节点的向量相同，则更新已存在节点的数据
                if (Objects.deepEquals(node.getItem().vector(), item.vector())) {
                    node.item = item;
                    return true;
                } else {
                    // 如果数据点的向量与已存在节点的向量不同，则删除已存在节点，以便添加新的数据点
                    remove(item.id(), item.version());
                }
            } else if (item.version() < deletedItemVersions.getIfAbsent(item.id(), -1)) {
                // 如果数据点已被删除，则不添加数据点
                return false;
            }

            // 检查索引是否已满
            if (nodeCount >= this.maxItemCount) {
                throw new SizeLimitExceededException("索引中的项数: " + nodeCount + ", 超过了最大限制: " + maxItemCount);
            }

            // 为新节点分配一个唯一的节点ID
            int newNodeId = nodeCount++;

            // 将新节点添加到排除候选集合中
            synchronized (excludedCandidates) {
                excludedCandidates.add(newNodeId);
            }

            // 创建新节点
            Node<TItem> newNode = new Node<>(newNodeId, connections, item, false);

            // 将新节点添加到节点数组中
            nodes.set(newNodeId, newNode);

            // 将数据点标识符与节点ID建立映射关系
            lookup.put(item.id(), newNodeId);

            // 从已删除数据点的版本记录中删除该数据点
            deletedItemVersions.remove(item.id());

            // 获取数据点的锁
            Object lock = itemLocks.computeIfAbsent(item.id(), k -> new Object());

            // 获取入口点节点的副本
            Node<TItem> entryPointCopy = entryPoint;

            try {
                // 获取数据点锁和新节点锁
                synchronized (lock) {
                    synchronized (newNode) {

                        // 如果入口点节点存在且新节点的层级小于等于入口点节点的最大层级，则释放全局锁
                        if (entryPoint != null && randomLevel <= entryPoint.maxLevel()) {
                            globalLock.unlock();
                        }

                        // 从入口点节点开始，向下搜索直到找到每个层级的最佳候选节点
                        Node<TItem> currObj = entryPointCopy;

                        if (currObj != null) {
                            if (newNode.maxLevel() < entryPointCopy.maxLevel()) {
                                TDistance curDist = distanceType.distance(item.vector(), currObj.item.vector());

                                for (int activeLevel = entryPointCopy.maxLevel(); activeLevel > newNode.maxLevel(); activeLevel--) {

                                    boolean changed = true;

                                    while (changed) {
                                        changed = false;

                                        synchronized (currObj) {
                                            MutableIntList candidateConnections = currObj.connections[activeLevel];

                                            for (int i = 0; i < candidateConnections.size(); i++) {

                                                int candidateId = candidateConnections.get(i);

                                                Node<TItem> candidateNode = nodes.get(candidateId);

                                                TDistance candidateDistance = distanceType.distance(
                                                        item.vector(),
                                                        candidateNode.item.vector()
                                                );

                                                if (lt(candidateDistance, curDist)) {
                                                    curDist = candidateDistance;
                                                    currObj = candidateNode;
                                                    changed = true;
                                                }
                                            }
                                        }
                                    }
                                }
                            }

                            // 在每个层级上搜索最佳候选节点，并将新节点与候选节点互相连接
                            for (int level = Math.min(randomLevel, entryPointCopy.maxLevel()); level >= 0; level--) {
                                PriorityQueue<NodeIdAndDistance<TDistance>> topCandidates = searchBaseLayer(currObj, item.vector(), efConstruction, level);

                                if (entryPointCopy.deleted) {
                                    TDistance distance = distanceType.distance(item.vector(), entryPointCopy.getItem().vector());
                                    topCandidates.add(new NodeIdAndDistance<>(entryPointCopy.id, distance, maxValueDistanceComparator));

                                    if (topCandidates.size() > efConstruction) {
                                        topCandidates.poll();
                                    }
                                }

                                mutuallyConnectNewElement(newNode, topCandidates, level);
                            }
                        }

                        // 如果新节点的最大层级大于入口点节点的最大层级，则将新节点设置为入口点节点
                        if (entryPoint == null || newNode.maxLevel() > entryPointCopy.maxLevel()) {
                            // 这是线程安全的，因为在添加level时获得了全局锁
                            this.entryPoint = newNode;
                        }
                        return true;
                    }
                }
            } finally {
                // 从排除候选集合中删除新节点
                synchronized (excludedCandidates) {
                    excludedCandidates.remove(newNodeId);
                }
            }
        } finally {
            // 释放全局锁
            if (globalLock.isHeldByCurrentThread()) {
                globalLock.unlock();
            }
        }
    }


    /**
     * 将新节点与候选节点互相连接
     *
     * @param newNode       新节点
     * @param topCandidates 候选节点
     * @param level         当前层级
     */
    private void mutuallyConnectNewElement(Node<TItem> newNode,
                                           PriorityQueue<NodeIdAndDistance<TDistance>> topCandidates,
                                           int level) {
        // 获取最佳候选节点的数量
        int bestN = level == 0 ? this.maxM0 : this.maxM;

        // 获取新节点的 ID 和向量
        int newNodeId = newNode.id;
        TVector newItemVector = newNode.getItem().vector();

        // 获取新节点在当前层级上的连接列表
        MutableIntList newItemConnections = newNode.connections[level];

        // 根据启发式算法获取最佳候选节点
        getNeighborsByHeuristic2(topCandidates, m);

        // 将新节点与每个最佳候选节点互相连接
        while (!topCandidates.isEmpty()) {
            // 获取当前最佳候选节点的 ID
            int selectedNeighbourId = topCandidates.poll().nodeId;

            // 如果当前最佳候选节点已经被排除，则跳过
            synchronized (excludedCandidates) {
                if (excludedCandidates.contains(selectedNeighbourId)) {
                    continue;
                }
            }

            // 将当前最佳候选节点添加到新节点的连接列表中
            newItemConnections.add(selectedNeighbourId);

            // 获取当前最佳候选节点
            Node<TItem> neighbourNode = nodes.get(selectedNeighbourId);

            // 对当前最佳候选节点进行同步操作
            synchronized (neighbourNode) {
                // 获取当前最佳候选节点的向量
                TVector neighbourVector = neighbourNode.getItem().vector();

                // 获取当前最佳候选节点在当前层级上的连接列表
                MutableIntList neighbourConnectionsAtLevel = neighbourNode.connections[level];

                // 如果当前最佳候选节点在当前层级上的连接列表未满，则将新节点添加到该列表中
                if (neighbourConnectionsAtLevel.size() < bestN) {
                    neighbourConnectionsAtLevel.add(newNodeId);
                } else {
                    // 找到被新的元素替换的“最弱的”元素
                    TDistance dMax = distanceType.distance(
                            newItemVector,
                            neighbourNode.getItem().vector()
                    );

                    // 创建一个优先队列，用于存储当前最佳候选节点和它的连接列表中的节点
                    Comparator<NodeIdAndDistance<TDistance>> comparator = Comparator.<NodeIdAndDistance<TDistance>>naturalOrder().reversed();
                    PriorityQueue<NodeIdAndDistance<TDistance>> candidates = new PriorityQueue<>(comparator);

                    // 将新节点添加到优先队列中
                    candidates.add(new NodeIdAndDistance<>(newNodeId, dMax, maxValueDistanceComparator));

                    // 将当前最佳候选节点的连接列表中的节点添加到优先队列中
                    neighbourConnectionsAtLevel.forEach(id -> {
                        TDistance dist = distanceType.distance(
                                neighbourVector,
                                nodes.get(id).getItem().vector()
                        );

                        candidates.add(new NodeIdAndDistance<>(id, dist, maxValueDistanceComparator));
                    });

                    // 根据启发式算法获取最佳候选节点
                    getNeighborsByHeuristic2(candidates, bestN);

                    // 清空当前最佳候选节点在当前层级上的连接列表
                    neighbourConnectionsAtLevel.clear();

                    // 将优先队列中的节点添加到当前最佳候选节点在当前层级上的连接列表中
                    while (!candidates.isEmpty()) {
                        neighbourConnectionsAtLevel.add(candidates.poll().nodeId);
                    }
                }
            }
        }
    }


    /**
     * 通过启发式算法获取最佳候选节点
     *
     * @param topCandidates 候选节点
     * @param m             最佳候选节点的数量
     */
    private void getNeighborsByHeuristic2(PriorityQueue<NodeIdAndDistance<TDistance>> topCandidates, int m) {

        // 如果候选节点数量小于最佳候选节点数量，则直接返回
        if (topCandidates.size() < m) {
            return;
        }

        // 创建一个按距离排序的优先队列
        PriorityQueue<NodeIdAndDistance<TDistance>> queueClosest = new PriorityQueue<>();
        // 创建一个用于存储最佳候选节点的列表
        List<NodeIdAndDistance<TDistance>> returnList = new ArrayList<>();

        // 将候选节点添加到按距离排序的优先队列中
        while (!topCandidates.isEmpty()) {
            queueClosest.add(topCandidates.poll());
        }

        // 从按距离排序的优先队列中选取最佳候选节点
        while (!queueClosest.isEmpty()) {
            // 如果已经选取了足够数量的最佳候选节点，则退出循环
            if (returnList.size() >= m) {
                break;
            }

            // 获取当前距离最近的候选节点
            NodeIdAndDistance<TDistance> currentPair = queueClosest.poll();

            // 获取当前距离最近的候选节点与查询节点之间的距离
            TDistance distToQuery = currentPair.distance;

            // 判断当前距离最近的候选节点是否为最佳候选节点
            boolean good = true;

            // 遍历已选取的最佳候选节点
            for (NodeIdAndDistance<TDistance> secondPair : returnList) {
                // 获取当前已选取的最佳候选节点与当前距离最近的候选节点之间的距离
                TDistance curDist = distanceType.distance(
                        nodes.get(secondPair.nodeId).getItem().vector(),
                        nodes.get(currentPair.nodeId).getItem().vector()
                );

                // 如果当前已选取的最佳候选节点与当前距离最近的候选节点之间的距离小于当前距离最近的候选节点与查询节点之间的距离，则当前距离最近的候选节点不是最佳候选节点
                if (lt(curDist, distToQuery)) {
                    good = false;
                    break;
                }
            }
            // 如果当前距离最近的候选节点是最佳候选节点，则将其添加到最佳候选节点列表中
            if (good) {
                returnList.add(currentPair);
            }
        }

        // 将最佳候选节点列表中的节点添加到候选节点列表中
        topCandidates.addAll(returnList);
    }

    /**
     * 从索引中删除指定 ID 的项
     *
     * @param id      要删除的项的 ID
     * @param version 要删除的项的版本号
     * @return 是否删除成功
     */
    @Override
    public boolean remove(TId id, long version) {
        // 如果删除操作未启用，则直接返回删除失败
        if (!removeEnabled) {
            return false;
        }

        // 获取全局锁
        globalLock.lock();

        try {
            // 查找指定 ID 对应的内部节点 ID
            int internalNodeId = lookup.getIfAbsent(id, NO_NODE_ID);
            // 如果指定 ID 对应的内部节点 ID 不存在，则直接返回删除失败
            if (internalNodeId == NO_NODE_ID) {
                return false;
            }

            // 获取指定 ID 对应的节点
            Node<TItem> node = nodes.get(internalNodeId);

            // 如果指定 ID 对应的节点不存在，则直接返回删除失败
            if (node == null) {
                return false;
            }

            // 如果指定 ID 对应的节点的版本号大于要删除的版本号，则直接返回删除失败
            if (node.getItem().version() > version) {
                return false;
            }

            // 将指定 ID 对应的节点标记为已删除
            node.deleted = true;

            // 从查找表中删除指定 ID
            lookup.remove(id);

            // 将被删除的项的版本号添加到已删除项版本号列表中
            deletedItemVersions.put(id, version);

            // 返回删除成功
            return true;
        } finally {
            // 释放全局锁
            globalLock.unlock();
        }
    }


    /**
     * 返回索引中的项数 该方法是线程安全的 但是它的返回值可能不是最新的
     * 因为在返回值之后可能会有新的项被添加到索引中
     *
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

    /**
     * 根据 ID 获取索引中的项
     *
     * @param id 要获取的项的 ID
     * @return 如果索引中存在指定 ID 的项，则返回该项，否则返回 Optional.empty()
     */
    @Override
    public Optional<TItem> get(TId id) {
        // 获取全局锁
        globalLock.lock();
        try {
            // 查找指定 ID 对应的内部节点 ID
            int nodeId = lookup.getIfAbsent(id, NO_NODE_ID);
            // 如果指定 ID 对应的内部节点 ID 不存在，则返回 Optional.empty()
            if (nodeId == NO_NODE_ID) {
                return Optional.empty();
            }
            // 返回指定 ID 对应的节点的项
            return Optional.ofNullable(nodes.get(nodeId)).map(Node::getItem);
        } finally {
            // 释放全局锁
            globalLock.unlock();
        }
    }


    /**
     * 返回索引中的所有项 该方法是线程安全的 但是它的返回值可能不是最新的
     * 因为在返回值之后可能会有新的项被添加到索引中
     *
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

    /**
     * 在HNSW索引中查找距离给定目标向量最近的k个邻居，并返回它们的搜索结果列表。
     *
     * @param destination 向量
     * @param k           数目
     * @return 搜索结果列表
     */
    @Override
    public List<SearchResultBO<TItem, TDistance>> findNearest(TVector destination, int k) {
        // 检查入口点是否为空
        if (entryPoint == null) {
            return Collections.emptyList();
        }

        // 创建入口点的副本
        Node<TItem> entryPointCopy = entryPoint;

        // 将当前对象设置为入口点的副本
        Node<TItem> curObj = entryPointCopy;

        // 计算目标向量与当前对象的初始距离
        TDistance curDist = distanceType.distance(destination, curObj.getItem().vector());

        // 从最高层开始向下遍历
        for (int activeLevel = entryPointCopy.maxLevel(); activeLevel > 0; activeLevel--) {
            boolean changed = true;

            // 循环知道没有距离更新
            while (changed) {
                changed = false;

                // 为当前对象加锁，确保多线程访问的安全性
                synchronized (curObj) {
                    // 获取当前层级的候选连接列表
                    MutableIntList candidateConnections = curObj.connections[activeLevel];

                    // 遍历候选连接列表
                    for (int i = 0; i < candidateConnections.size(); i++) {

                        // 获取候选连接的节点ID
                        int candidateId = candidateConnections.get(i);

                        // 计算目标向量与候选连接的距离
                        TDistance candidateDist = distanceType.distance(
                                destination,
                                nodes.get(candidateId).getItem().vector()
                        );

                        // 如果候选连接的距离小于当前距离，则更新当前距离，并改变标记
                        if (lt(candidateDist, curDist)) {
                            curObj = nodes.get(candidateId);
                            curDist = candidateDist;
                            changed = true;
                        }
                    }
                }
            }
        }

        // 在基础层级上进行搜索，获取最近的候选对象的优先级队列
        PriorityQueue<NodeIdAndDistance<TDistance>> topCandidates = searchBaseLayer(
                curObj,
                destination,
                Math.max(ef, k),
                0
        );

        // 如果队列的大小超过k，则移除距离最大的元素，保持队列的大小为k
        while (topCandidates.size() > k) {
            topCandidates.poll();
        }

        // 将队列中的元素转换为搜索结果列表
        List<SearchResultBO<TItem, TDistance>> results = new ArrayList<>(topCandidates.size());
        while (!topCandidates.isEmpty()) {
            NodeIdAndDistance<TDistance> pair = topCandidates.poll();
            results.add(0, new SearchResultBO<>(pair.distance, nodes.get(pair.nodeId).getItem(), maxValueDistanceComparator));
        }

        return results;
    }

    /**
     * 将 HNSW 索引保存到输出流中
     *
     * @param out 输出流
     * @throws IOException 如果写入输出流时发生错误
     */
    @Override
    public void save(OutputStream out) throws IOException {
        try (ObjectOutputStream oos = new ObjectOutputStream(out)) {
            oos.writeObject(this);
        }
    }


    /**
     * 在 HNSW 索引中搜索基础层级，返回最近的候选对象的优先级队列
     *
     * @param entryPointNode 入口点
     * @param destination    目标向量
     * @param k              数目
     * @param layer          层级
     * @return 最近的候选对象的优先级队列
     */
    private PriorityQueue<NodeIdAndDistance<TDistance>> searchBaseLayer(
            Node<TItem> entryPointNode,
            TVector destination,
            int k,
            int layer
    ) {
        // 创建已访问过的节点的位集合对象
        ArrayBitSet visitedBitSet = visitedBitSetPool.borrowObject();

        try {
            // 创建两个优先级队列，一个用于存储最近的候选对象，一个用于存储所有候选对象
            PriorityQueue<NodeIdAndDistance<TDistance>> topCandidates =
                    new PriorityQueue<>(Comparator.<NodeIdAndDistance<TDistance>>naturalOrder().reversed());
            PriorityQueue<NodeIdAndDistance<TDistance>> candidateSet = new PriorityQueue<>();

            TDistance lowerBound;

            // 如果入口节点未被删除
            if (!entryPointNode.deleted) {
                // 计算目标向量与入口节点的距离，并创建一个NodeIdAndDistance对象
                TDistance distance = distanceType.distance(destination, entryPointNode.getItem().vector());
                NodeIdAndDistance<TDistance> pair = new NodeIdAndDistance<>(entryPointNode.id, distance, maxValueDistanceComparator);

                topCandidates.add(pair);
                lowerBound = distance;
                candidateSet.add(pair);
            } else {
                // 如果入口节点已被删除，设置下界为最大值
                lowerBound = MaxValueComparator.maxValue();
                NodeIdAndDistance<TDistance> pair = new NodeIdAndDistance<>(entryPointNode.id, lowerBound, maxValueDistanceComparator);
                candidateSet.add(pair);
            }

            // 将入口点节点标记为已访问
            visitedBitSet.add(entryPointNode.id);

            while (!candidateSet.isEmpty()) {
                // 从候选节点集合中取出距离最近的节点
                NodeIdAndDistance<TDistance> currentPair = candidateSet.poll();

                // 如果当前节点的距离大于下界，则跳出循环
                if (gt(currentPair.distance, lowerBound)) {
                    break;
                }

                // 获取当前节点对象
                Node<TItem> node = nodes.get(currentPair.nodeId);

                // 对当前节点加锁，确保多线程访问的安全性
                synchronized (node) {

                    // 获取当前节点在指定层级的连接列表
                    MutableIntList candidates = node.connections[layer];

                    // 遍历连接列表中的候选节点
                    for (int i = 0; i < candidates.size(); i++) {

                        int candidateId = candidates.get(i);

                        // 如果候选节点未被访问过
                        if (!visitedBitSet.contains(candidateId)) {

                            // 将候选节点标记为已访问
                            visitedBitSet.add(candidateId);

                            // 获取候选节点对象
                            Node<TItem> candidateNode = nodes.get(candidateId);

                            // 计算目标向量与候选节点的距离
                            TDistance candidateDistance = distanceType.distance(destination, candidateNode.getItem().vector());

                            // 如果最近邻候选集的大小小于k或者候选节点的距离小于下界
                            if (topCandidates.size() < k || gt(lowerBound, candidateDistance)) {

                                // 创建一个NodeIdAndDistance对象，并将其添加到候选集合中
                                NodeIdAndDistance<TDistance> candidatePair = new NodeIdAndDistance<>(candidateId, candidateDistance, maxValueDistanceComparator);
                                candidateSet.add(candidatePair);

                                // 如果候选节点未被删除，则将其添加到最近邻候选集中
                                if (!candidateNode.deleted) {
                                    topCandidates.add(candidatePair);
                                }

                                // 如果最近邻候选集的大小大于k，则移除距离最大的元素，保持队列的大小为k
                                if (topCandidates.size() > k) {
                                    topCandidates.poll();
                                }

                                // 如果最近邻候选集非空，更新下界为最近邻候选集中距离最小的节点的距离
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
            // 清空已访问过的节点的位集合对象，并将其返回到对象池中
            visitedBitSet.clear();
            visitedBitSetPool.returnObject(visitedBitSet);
        }
    }

    /**
     * 创建一个只读的视图，包含距离搜索时成对比较的前k个最近邻居。
     * 可以用作评测搜索精确度的基准
     * 该方法将比较慢，但是每次都能给出正确的结果
     *
     * @return 一个只读的视图
     */
    public Index<TId, TVector, TItem, TDistance> asExactIndex() {
        return exactView;
    }

    /**
     * 返回存在当前Index下的items的维度
     *
     * @return 维度
     */
    public int getDimensions() {
        return dimensions;
    }

    /**
     * 返回新的节点加入时的双向连接数量
     *
     * @return 双向连接数量
     */
    public int getM() {
        return m;
    }

    /**
     * 返回搜索期间，最近邻节点的动态列表的大小
     *
     * @return 最近邻节点的动态列表的大小
     */
    public int getEf() {
        return ef;
    }

    /**
     * 设置搜索期间，最近邻节点的动态列表的大小
     *
     * @param ef 最近邻节点的动态列表的大小
     */
    public void setEf(int ef) {
        this.ef = ef;
    }

    /**
     * 返回相同含义的{@link #getEf()}ef，但是控制 index时间/index精度
     *
     * @return
     */
    public int getEfConstruction() {
        return efConstruction;
    }

    /**
     * 返回距离函数的类型
     *
     * @return 距离函数的类型
     */
    public DistanceType<TVector, TDistance> getDistanceType() {
        return distanceType;
    }

    /**
     * 返回距离比较器
     *
     * @return 距离比较器
     */
    public Comparator<TDistance> getDistanceComparator() {
        return distanceComparator;
    }

    /**
     * 返回删除是否可用
     *
     * @return 删除是否可用
     */
    public boolean isRemoveEnabled() {
        return removeEnabled;
    }

    /**
     * 返回当前Index可以保持的最大节点的数量
     *
     * @return 最大节点的数量
     */
    public int getMaxItemCount() {
        return maxItemCount;
    }

    /**
     * 返回当保存Index时，用于序列化Item的id的序列化器
     *
     * @return 序列化器
     */
    public ObjectSerializer<TId> getItemIdSerializer() {
        return itemIdSerializer;
    }

    /**
     * 返回当保存Index时，用于序列化Item的vector的序列化器
     *
     * @return 序列化器
     */
    public ObjectSerializer<TItem> getItemSerializer() {
        return itemSerializer;
    }

    /**
     * 将Index对象序列化到输出流中
     *
     * @param objectOutputStream 输出流
     * @throws IOException IO异常
     */
    private void writeObject(ObjectOutputStream objectOutputStream) throws IOException {
        // 写入版本号
        objectOutputStream.writeByte(VERSION_1);
        // 写入维度
        objectOutputStream.writeInt(dimensions);
        // 写入距离类型
        objectOutputStream.writeObject(distanceType);
        // 写入距离比较器
        objectOutputStream.writeObject(distanceComparator);
        // 写入id序列化器
        objectOutputStream.writeObject(itemIdSerializer);
        // 写入item序列化器
        objectOutputStream.writeObject(itemSerializer);
        // 写入最大节点数量
        objectOutputStream.writeInt(maxItemCount);
        // 写入m值
        objectOutputStream.writeInt(m);
        // 写入最大m值
        objectOutputStream.writeInt(maxM);
        // 写入最大m0值
        objectOutputStream.writeInt(maxM0);
        // 写入levelLambda值
        objectOutputStream.writeDouble(levelLambda);
        // 写入ef值
        objectOutputStream.writeInt(ef);
        // 写入efConstruction值
        objectOutputStream.writeInt(efConstruction);
        // 写入删除是否可用
        objectOutputStream.writeBoolean(removeEnabled);
        // 写入节点数量
        objectOutputStream.writeInt(nodeCount);
        // 写入lookup
        writeMutableObjectIntMap(objectOutputStream, lookup);
        // 写入删除的item版本
        writeMutableObjectLongMap(objectOutputStream, deletedItemVersions);
        // 写入节点数组
        writeNodesArray(objectOutputStream, nodes);
        // 写入entryPoint
        objectOutputStream.writeInt(entryPoint == null ? -1 : entryPoint.id);
    }

    /**
     * 从输入流中反序列化Index对象
     *
     * @param objectInputStream 输入流
     * @throws IOException            IO异常
     * @throws ClassNotFoundException 类未找到异常
     */
    @SuppressWarnings("unchecked")
    private void readObject(ObjectInputStream objectInputStream) throws IOException, ClassNotFoundException {
        // 读取版本号，用于应对未来不兼容的序列化版本
        @SuppressWarnings("unused") byte version = objectInputStream.readByte();
        // 读取维度
        this.dimensions = objectInputStream.readInt();
        // 读取距离类型
        this.distanceType = (DistanceType<TVector, TDistance>) objectInputStream.readObject();
        // 读取距离比较器
        this.distanceComparator = (Comparator<TDistance>) objectInputStream.readObject();
        // 创建最大值比较器
        this.maxValueDistanceComparator = new MaxValueComparator<>(distanceComparator);
        // 读取id序列化器
        this.itemIdSerializer = (ObjectSerializer<TId>) objectInputStream.readObject();
        // 读取item序列化器
        this.itemSerializer = (ObjectSerializer<TItem>) objectInputStream.readObject();
        // 读取最大节点数量
        this.maxItemCount = objectInputStream.readInt();
        // 读取m值
        this.m = objectInputStream.readInt();
        // 读取最大m值
        this.maxM = objectInputStream.readInt();
        // 读取最大m0值
        this.maxM0 = objectInputStream.readInt();
        // 读取levelLambda值
        this.levelLambda = objectInputStream.readDouble();
        // 读取ef值
        this.ef = objectInputStream.readInt();
        // 读取efConstruction值
        this.efConstruction = objectInputStream.readInt();
        // 读取删除是否可用
        this.removeEnabled = objectInputStream.readBoolean();
        // 读取节点数量
        this.nodeCount = objectInputStream.readInt();
        // 读取lookup
        this.lookup = readMutableObjectIntMap(objectInputStream, itemIdSerializer);
        // 读取删除的item版本
        this.deletedItemVersions = readMutableObjectLongMap(objectInputStream, itemIdSerializer);
        // 读取节点数组
        this.nodes = readNodesArray(objectInputStream, itemSerializer, maxM0, maxM);
        // 读取entryPoint
        int entryPointNodeId = objectInputStream.readInt();
        this.entryPoint = entryPointNodeId == -1 ? null : nodes.get(entryPointNodeId);

        // 初始化全局锁
        this.globalLock = new ReentrantLock();
        // 初始化已访问过的节点的位集合对象池
        this.visitedBitSetPool = new GenericObjectPool<>(() -> new ArrayBitSet(this.maxItemCount),
                Runtime.getRuntime().availableProcessors());
        // 初始化排除候选集合
        this.excludedCandidates = new ArrayBitSet(this.maxItemCount);
        // 初始化item锁
        this.itemLocks = new HashMap<>();
        // 初始化只读视图
        this.exactView = new ExactView();
    }

    /**
     * 将可变对象和整数映射写入对象输出流中
     *
     * @param objectOutputStream 对象输出流
     * @param map                可变对象和整数映射
     * @throws IOException IO异常
     */
    private void writeMutableObjectIntMap(ObjectOutputStream objectOutputStream, MutableObjectIntMap<TId> map) throws IOException {
        // 写入映射的大小
        objectOutputStream.writeInt(map.size());

        // 遍历映射中的键值对，将键和值写入对象输出流中
        for (ObjectIntPair<TId> pair : map.keyValuesView()) {
            // 写入键
            itemIdSerializer.write(pair.getOne(), objectOutputStream);
            // 写入值
            objectOutputStream.writeInt(pair.getTwo());
        }
    }


    /**
     * 将可变对象和长整型映射写入对象输出流中
     *
     * @param objectOutputStream 对象输出流
     * @param map                可变对象和长整型映射
     * @throws IOException IO异常
     */
    private void writeMutableObjectLongMap(ObjectOutputStream objectOutputStream, MutableObjectLongMap<TId> map) throws IOException {
        // 写入映射的大小
        objectOutputStream.writeInt(map.size());

        // 遍历映射中的键值对，将键和值写入对象输出流中
        for (ObjectLongPair<TId> pair : map.keyValuesView()) {
            // 写入键
            itemIdSerializer.write(pair.getOne(), objectOutputStream);
            // 写入值
            objectOutputStream.writeLong(pair.getTwo());
        }
    }


    /**
     * 将节点数组写入对象输出流中
     *
     * @param objectOutputStream 对象输出流
     * @param nodes              节点数组
     * @throws IOException IO异常
     */
    private void writeNodesArray(ObjectOutputStream objectOutputStream, AtomicReferenceArray<Node<TItem>> nodes)
            throws IOException {
        // 写入节点数组的长度
        objectOutputStream.writeInt(nodes.length());

        // 遍历节点数组，将每个节点写入对象输出流中
        for (int i = 0; i < nodes.length(); i++) {
            writeNode(objectOutputStream, nodes.get(i));
        }
    }


    /**
     * 将节点写入对象输出流中
     *
     * @param objectOutputStream 对象输出流
     * @param node               节点
     * @throws IOException IO异常
     */
    private void writeNode(ObjectOutputStream objectOutputStream, Node<TItem> node) throws IOException {
        if (node == null) {
            // 如果节点为空，写入-1
            objectOutputStream.writeInt(-1);
        } else {
            // 否则，写入节点id和连接数
            objectOutputStream.writeInt(node.id);
            objectOutputStream.writeInt(node.connections.length);

            // 遍历节点的连接，将每个连接写入对象输出流中
            for (MutableIntList connection : node.connections) {
                // 写入连接的大小
                objectOutputStream.writeInt(connection.size());

                // 遍历连接中的每个元素，将每个元素写入对象输出流中
                for (int i = 0; i < connection.size(); i++) {
                    objectOutputStream.writeInt(connection.get(i));

                    // 遍历连接中的每个元素，将每个元素写入对象输出流中
                    for (int j = 0; j < connection.size(); j++) {
                        objectOutputStream.writeInt(connection.get(j));
                    }
                }
                // 写入节点的item
                itemSerializer.write(node.item, objectOutputStream);
                // 写入节点是否被删除
                objectOutputStream.writeBoolean(node.deleted);
            }
        }
    }


    /**
     * 从文件中载入索引 HnswIndex
     *
     * @param file        文件 File
     * @param <TId>       id 类型
     * @param <TVector>   向量类型
     * @param <TItem>     item 类型
     * @param <TDistance> 距离类型
     * @return 索引 HnswIndex
     * @throws IOException IO 异常
     */
    public static <TId, TVector, TItem extends Item<TId, TVector>, TDistance> HnswIndex<TId, TVector, TItem, TDistance> load(File file)
            throws IOException {
        return load(new FileInputStream(file));
    }

    /**
     * 从文件中载入索引 HnswIndex
     *
     * @param file        文件 File
     * @param classLoader 类加载器 ClassLoader
     * @param <TId>       id 类型
     * @param <TVector>   向量类型
     * @param <TItem>     item 类型
     * @param <TDistance> 距离类型
     * @return 索引 HnswIndex
     * @throws IOException IO 异常
     */
    public static <TId, TVector, TItem extends Item<TId, TVector>, TDistance> HnswIndex<TId, TVector, TItem, TDistance> load(File file, ClassLoader classLoader)
            throws IOException {
        return load(new FileInputStream(file), classLoader);
    }

    /**
     * 从路径中载入索引 HnswIndex
     *
     * @param path        路径 Path
     * @param <TId>       id 类型
     * @param <TVector>   向量类型
     * @param <TItem>     item 类型
     * @param <TDistance> 距离类型
     * @return 索引 HnswIndex
     * @throws IOException IO 异常
     */
    public static <TId, TVector, TItem extends Item<TId, TVector>, TDistance> HnswIndex<TId, TVector, TItem, TDistance> load(Path path)
            throws IOException {
        return load(Files.newInputStream(path));
    }

    /**
     * 从路径中载入索引 HnswIndex
     *
     * @param path        路径 Path
     * @param classLoader 类加载器 ClassLoader
     * @param <TId>       id 类型
     * @param <TVector>   向量类型
     * @param <TItem>     item 类型
     * @param <TDistance> 距离类型
     * @return 索引 HnswIndex
     * @throws IOException IO 异常
     */
    public static <TId, TVector, TItem extends Item<TId, TVector>, TDistance> HnswIndex<TId, TVector, TItem, TDistance> load(Path path, ClassLoader classLoader)
            throws IOException {
        return load(Files.newInputStream(path), classLoader);
    }

    /**
     * 从输入流中载入索引 HnswIndex
     *
     * @param inputStream 输入流
     * @param <TId>       id 类型
     * @param <TVector>   向量类型
     * @param <TItem>     item 类型
     * @param <TDistance> 距离类型
     * @return 索引 HnswIndex
     * @throws IOException              IO 异常
     * @throws IllegalArgumentException 找不到用于载入的文件
     */
    public static <TId, TVector, TItem extends Item<TId, TVector>, TDistance> HnswIndex<TId, TVector, TItem, TDistance> load(InputStream inputStream)
            throws IOException {
        return load(inputStream, Thread.currentThread().getContextClassLoader());
    }

    /**
     * 从输入流中载入索引 HnswIndex
     *
     * @param inputStream 输入流
     * @param classLoader 类加载器
     * @param <TId>       id 类型
     * @param <TVector>   向量类型
     * @param <TItem>     item 类型
     * @param <TDistance> 距离类型
     * @return 索引 HnswIndex
     * @throws IOException              IO 异常
     * @throws IllegalArgumentException 找不到用于载入的文件
     */
    @SuppressWarnings("unchecked")
    public static <TId, TVector, TItem extends Item<TId, TVector>, TDistance> HnswIndex<TId, TVector, TItem, TDistance> load(InputStream inputStream, ClassLoader classLoader)
            throws IOException {
        try (ObjectInputStream ois = new ClassLoaderObjectInputStream(classLoader, inputStream)) {
            return (HnswIndex<TId, TVector, TItem, TDistance>) ois.readObject();
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException("找不到用于载入的文件", e);
        }
    }

    /**
     * 从 ObjectInputStream 中读取 IntArrayList
     *
     * @param ois         ObjectInputStream 对象
     * @param initialSize IntArrayList 的初始大小
     * @return 读取到的 IntArrayList 对象
     * @throws IOException IO 异常
     */
    private static IntArrayList readIntArrayList(ObjectInputStream ois, int initialSize) throws IOException {
        // 读取 IntArrayList 的大小
        int size = ois.readInt();

        // 创建 IntArrayList 对象
        IntArrayList list = new IntArrayList(initialSize);

        // 读取 IntArrayList 中的元素
        for (int j = 0; j < size; j++) {
            list.add(ois.readInt());
        }

        return list;
    }


    /**
     * 从 ObjectInputStream 中读取 Node 对象
     *
     * @param ois            ObjectInputStream 对象
     * @param itemSerializer ItemSerializer 对象
     * @param maxM0          0 级最大 M 值
     * @param maxM           最大 M 值
     * @param <TItem>        item 类型
     * @return 读取到的 Node 对象
     * @throws IOException            IO 异常
     * @throws ClassNotFoundException 找不到类异常
     */
    private static <TItem> Node<TItem> readNode(ObjectInputStream ois,
                                                ObjectSerializer<TItem> itemSerializer,
                                                int maxM0,
                                                int maxM) throws IOException, ClassNotFoundException {

        int id = ois.readInt();

        if (id == -1) {
            return null;
        } else {
            int connectionsSize = ois.readInt();

            MutableIntList[] connections = new MutableIntList[connectionsSize];

            for (int i = 0; i < connectionsSize; i++) {
                int levelM = i == 0 ? maxM0 : maxM;
                connections[i] = readIntArrayList(ois, levelM);
            }

            TItem item = itemSerializer.read(ois);

            boolean deleted = ois.readBoolean();

            return new Node<>(id, connections, item, deleted);
        }
    }

    /**
     * 从 ObjectInputStream 中读取 AtomicReferenceArray<Node> 对象
     *
     * @param ois            ObjectInputStream 对象
     * @param itemSerializer ItemSerializer 对象
     * @param maxM0          0 级最大 M 值
     * @param maxM           最大 M 值
     * @param <TItem>        item 类型
     * @return 读取到的 AtomicReferenceArray<Node> 对象
     * @throws IOException            IO 异常
     * @throws ClassNotFoundException 找不到类异常
     */
    private static <TItem> AtomicReferenceArray<Node<TItem>> readNodesArray(ObjectInputStream ois,
                                                                            ObjectSerializer<TItem> itemSerializer,
                                                                            int maxM0,
                                                                            int maxM)
            throws IOException, ClassNotFoundException {

        int size = ois.readInt();
        AtomicReferenceArray<Node<TItem>> nodes = new AtomicReferenceArray<>(size);

        for (int i = 0; i < nodes.length(); i++) {
            nodes.set(i, readNode(ois, itemSerializer, maxM0, maxM));
        }

        return nodes;
    }

    /**
     * 从 ObjectInputStream 中读取 MutableObjectIntMap 对象
     *
     * @param ois              ObjectInputStream 对象
     * @param itemIdSerializer ItemIdSerializer 对象
     * @param <TId>            id 类型
     * @return 读取到的 MutableObjectIntMap 对象
     * @throws IOException            IO 异常
     * @throws ClassNotFoundException 找不到类异常
     */
    private static <TId> MutableObjectIntMap<TId> readMutableObjectIntMap(ObjectInputStream ois,
                                                                          ObjectSerializer<TId> itemIdSerializer)
            throws IOException, ClassNotFoundException {

        int size = ois.readInt();

        MutableObjectIntMap<TId> map = new ObjectIntHashMap<>(size);

        for (int i = 0; i < size; i++) {
            TId key = itemIdSerializer.read(ois);
            int value = ois.readInt();

            map.put(key, value);
        }
        return map;
    }

    /**
     * 从 ObjectInputStream 中读取 MutableObjectLongMap 对象
     *
     * @param ois              ObjectInputStream 对象
     * @param itemIdSerializer ItemIdSerializer 对象
     * @param <TId>            id 类型
     * @return 读取到的 MutableObjectLongMap 对象
     * @throws IOException            IO 异常
     * @throws ClassNotFoundException 找不到类异常
     */
    private static <TId> MutableObjectLongMap<TId> readMutableObjectLongMap(ObjectInputStream ois,
                                                                            ObjectSerializer<TId> itemIdSerializer)
            throws IOException, ClassNotFoundException {

        int size = ois.readInt();

        MutableObjectLongMap<TId> map = new ObjectLongHashMap<>(size);

        for (int i = 0; i < size; i++) {
            TId key = itemIdSerializer.read(ois);
            long value = ois.readLong();

            map.put(key, value);
        }
        return map;
    }

    /**
     * 由新建一个HNSW index开始程序
     *
     * @param dimensions   向量维度
     * @param distanceType 距离函数
     * @param maxItemCount 最大item数量
     * @param <TVector>    向量类型
     * @param <TDistance>  距离类型
     * @return Builder
     */
    public static <TVector, TDistance extends Comparable<TDistance>> Builder<TVector, TDistance> newBuilder(
            int dimensions,
            DistanceType<TVector, TDistance> distanceType,
            int maxItemCount) {

        Comparator<TDistance> distanceComparator = Comparator.naturalOrder();

        return new Builder<>(dimensions, distanceType, distanceComparator, maxItemCount);
    }

    /**
     * 由新建一个HNSW index开始程序
     *
     * @param dimensions         向量维度
     * @param distanceType       距离函数
     * @param distanceComparator 距离比较器
     * @param maxItemCount       最大item数量
     * @param <TVector>          向量类型
     * @param <TDistance>        距离类型
     * @return Builder
     */
    public static <TVector, TDistance> Builder<TVector, TDistance> newBuilder(
            int dimensions,
            DistanceType<TVector, TDistance> distanceType,
            Comparator<TDistance> distanceComparator,
            int maxItemCount) {

        return new Builder<>(dimensions, distanceType, distanceComparator, maxItemCount);
    }

    /**
     * resize新的大小 重新分配内存
     *
     * @param newSize 新的大小
     */
    public void resize(int newSize) {
        globalLock.lock();
        try {
            this.maxItemCount = newSize;

            this.visitedBitSetPool = new GenericObjectPool<>(() -> new ArrayBitSet(this.maxItemCount),
                    Runtime.getRuntime().availableProcessors());

            AtomicReferenceArray<Node<TItem>> newNodes = new AtomicReferenceArray<>(newSize);

            for (int i = 0; i < this.nodes.length(); i++) {
                newNodes.set(i, this.nodes.get(i));
            }
            this.nodes = newNodes;

            this.excludedCandidates = new ArrayBitSet(this.excludedCandidates, newSize);
        } finally {
            globalLock.unlock();
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
         *
         * @return 是否还有下一个元素
         */
        @Override
        public boolean hasNext() {
            return done < HnswIndex.this.size();
        }

        /**
         * next()方法的实现
         *
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
     * 根据给定的值和参数 lambda 分配一个层级值。用于在 HNSW 索引结构中确定节点的连接和搜索路径。
     *
     * @param value  要分配层级的值
     * @param lambda 控制层级分配的参数
     * @return 分配的层级值
     */
    private int assignLevel(TId value, double lambda) {
        // 计算值的哈希码
        int hashCode = value.hashCode();

        byte[] bytes = {
                (byte) (hashCode >>> 24), // 取哈希码的高8位字节
                (byte) (hashCode >>> 16), // 取哈希码的次高8位字节
                (byte) (hashCode >>> 8), // 取哈希码的次低8位字节
                (byte) hashCode // 取哈希码的低8位字节
        };
        // // 使用 Murmur3 哈希函数生成随机数
        double random = Math.abs((double) Murmur3.hash32(bytes) / Integer.MAX_VALUE);

        // // 根据随机数的对数值乘以 lambda，得到层级值
        return (int) (-Math.log(random) * lambda);
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

    /**
     * 用于存储节点ID和距离的类
     *
     * @param <TDistance> 距离类型
     */
    static class NodeIdAndDistance<TDistance> implements Comparable<NodeIdAndDistance<TDistance>> {

        /**
         * 节点ID
         */
        final int nodeId;

        /**
         * 距离
         */
        final TDistance distance;

        /**
         * 距离比较器
         */
        final Comparator<TDistance> distanceComparator;

        /**
         * 构造函数
         *
         * @param nodeId             节点ID
         * @param distance           距离
         * @param distanceComparator 距离比较器
         */
        NodeIdAndDistance(int nodeId, TDistance distance, Comparator<TDistance> distanceComparator) {
            this.nodeId = nodeId;
            this.distance = distance;
            this.distanceComparator = distanceComparator;
        }

        /**
         * 用于比较两个NodeIdAndDistance对象的距离大小
         *
         * @param o 另一个NodeIdAndDistance对象
         * @return 如果当前对象的距离小于另一个对象的距离，则返回负整数；如果当前对象的距离等于另一个对象的距离，则返回0；否则返回正整数
         */
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
         *
         * @param item item
         * @return 是否成功
         */
        @Override
        public boolean add(TItem item) {
            return HnswIndex.this.add(item);
        }

        /**
         * 删除一个item
         *
         * @param id      item的id
         * @param version item的版本
         * @return 是否成功
         */
        @Override
        public boolean remove(TId id, long version) {
            return HnswIndex.this.remove(id, version);
        }

        /**
         * 获取item的数量
         *
         * @return item的数量
         */
        @Override
        public int size() {
            return HnswIndex.this.size();
        }

        @Override
        public Optional<TItem> get(TId id) {
            return HnswIndex.this.get(id);
        }

        /**
         * 获取所有的item
         *
         * @return 所有的item
         */
        @Override
        public Collection<TItem> items() {
            return HnswIndex.this.items();
        }

        /**
         * 查找最近的向量k个 这是一个精确的方法，它遍历所有的向量。
         *
         * @param vector 向量
         * @param k      数目
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
         *
         * @param out Index的输出流
         * @throws IOException IO异常
         */
        @Override
        public void save(OutputStream out) throws IOException {
            HnswIndex.this.save(out);
        }

        /**
         * 保存Index到文件
         *
         * @param file Index的文件
         * @throws IOException IO异常
         */
        @Override
        public void save(File file) throws IOException {
            HnswIndex.this.save(file);
        }

        /**
         * 保存Index到路径
         *
         * @param path Index的路径
         * @throws IOException IO异常
         */
        @Override
        public void save(Path path) throws IOException {
            HnswIndex.this.save(path);
        }

        /**
         * 添加所有的item
         *
         * @param items item的集合
         * @throws InterruptedException 中断异常
         */
        @Override
        public void addAll(Collection<TItem> items) throws InterruptedException {
            HnswIndex.this.addAll(items);
        }

        /**
         * 添加所有的item
         *
         * @param items    item的集合
         * @param listener 进度监听器
         * @throws InterruptedException 中断异常
         */
        @Override
        public void addAll(Collection<TItem> items, ProgressListener listener) throws InterruptedException {
            HnswIndex.this.addAll(items, listener);
        }

        /**
         * 添加所有的item
         *
         * @param items      item的集合
         * @param numThreads 线程数
         * @param listener   进度监听器
         * @throws InterruptedException 中断异常
         */
        public void addAll(Collection<TItem> items, int numThreads, ProgressListener listener, int progressUpdateInterval) throws InterruptedException {
            HnswIndex.this.addAll(items, numThreads, listener, progressUpdateInterval);
        }
    }

    /**
     * 节点
     *
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

        Node(int id, MutableIntList[] connections, TItem item, boolean deleted) {
            this.id = id;
            this.connections = connections;
            this.item = item;
            this.deleted = deleted;
        }

        /**
         * 节点的最大层级
         *
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
         *
         * @param delegate 用于比较距离的比较器
         */
        MaxValueComparator(Comparator<TDistance> delegate) {
            this.delegate = delegate;
        }

        /**
         * 比较两个对象。如果第一个对象大于第二个对象，则返回正整数；如果第一个对象小于第二个对象，则返回负整数；如果两个对象相等，则返回0。
         *
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
     *
     * @param <TVector>   向量的类型
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
         *
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
     *
     * @param <TId>       ID的类型
     * @param <TVector>   向量的类型
     * @param <TItem>     Item的类型
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
         *
         * @return Index
         */
        public RefinedBuilder<TId, TVector, TItem, TDistance> withCustomSerializers(
                ObjectSerializer<TId> itemIdSerializer,
                ObjectSerializer<TItem> itemSerializer
        ) {
            this.itemIdSerializer = itemIdSerializer;
            this.itemSerializer = itemSerializer;

            return this;
        }

        /**
         * 构建Index
         *
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
     *
     * @param <TBuilder>  构建器的类型
     * @param <TVector>   向量的类型
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
         */
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
         */
        int efConstruction = DEFAULT_EF_CONSTRUCTION;

        /**
         * 用于启用实验性的删除操作的标志
         */
        boolean removeEnabled = DEFAULT_REMOVE_ENABLED;

        /**
         * 构造方法
         *
         * @param dimensions         向量的维度
         * @param distanceType       距离的类型
         * @param distanceComparator 用于比较距离的比较器
         * @param maxItemCount       最大的Item数量
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
         *
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
         *
         * @param removeEnabled 是否允许删除节点
         * @return 构建器
         */
        public TBuilder withRemoveEnabled(boolean removeEnabled) {
            this.removeEnabled = removeEnabled;
            return self();
        }
    }

}
