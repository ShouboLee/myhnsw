package com.shoubo.hnsw;

import com.shoubo.Index;
import com.shoubo.Item;
import com.shoubo.model.bo.SearchResultBO;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

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
}
