package com.shoubo.model;

import lombok.Data;

import java.io.Serializable;

/**
 * Author: shoubo
 * Date: 2023/5/27
 * Desc: 稀疏向量实体类
 */
@Data
public class SparseVector<TVector> implements Serializable {
    /**
     * 序列化版本号
     */
    private static final long serialVersionUID = 1L;

    /**
     * 向量的维度
     */
    private int[] indices;
    /**
     * 向量的值
     */
    private TVector values;

}
