package com.shoubo.model;

import java.io.Serializable;

/**
 * Author: shoubo
 * Date: 2023/5/27
 * Desc: 距离函数，计算两向量之间的距离
 *
 * @param <TVector> 向量类型
 * @param <TDistance> 距离类型
 */
@FunctionalInterface
public interface Distance<TVector, TDistance> extends Serializable {

    /**
     * 计算两向量之间的距离
     * @param u 向量 u
     * @param v 向量 v
     * @return 向量 u 和 v 之间的距离
     */
    TDistance distance(TVector u, TVector v);
}
