package com.shoubo.hnsw;

import com.shoubo.Index;
import com.shoubo.Item;
import com.shoubo.model.DistanceType;
import com.shoubo.model.bo.SearchResultBO;
import com.shoubo.serializer.ObjectSerializer;
import com.shoubo.utils.ArrayBitSet;
import com.shoubo.utils.GenericObjectPool;
import lombok.Data;
import org.eclipse.collections.api.list.primitive.MutableIntList;
import org.eclipse.collections.api.map.primitive.MutableObjectIntMap;
import org.eclipse.collections.api.map.primitive.MutableObjectLongMap;

import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;
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

    private int removeEnabled;

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

    @Override
    public boolean add(TItem item) {
        return false;
    }

    @Override
    public boolean remove(TItem item, long version) {
        return false;
    }

    @Override
    public int size() {
        return 0;
    }

    @Override
    public Optional<TItem> get(TId id) {
        return Optional.empty();
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
}
