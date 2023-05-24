package com.shoubo;

import java.io.Serializable;

/**
 * Author: shoubo
 * Date: 2023/5/24
 * Desc: 可索引的项 该接口定义了一些方法，
 * 能够在具体的项中获取标识符、向量以及其他相关的信息。
 * @param <TId> Type of the external identifier of an item
 * @param <TVector> Type of the vector to perform distance calculation on
 */
public interface Item <TId, TVector> extends Serializable {
    /*
    * 返回项的标识符，即项的外部唯一标识。
    * */
    TId id();

    /*
    * 返回用于计算距离的向量。这个方法提供了获取项的向量表示的能力。
    * */
    TVector vector();

    /*
    * 返回向量的维度。这个方法返回项的向量的维度。
    * */
    int dimensions();

    /*
    * 返回项的版本。版本号用于表示项的变更情况，较高的版本号表示较新的项。
    * 默认情况下，版本号为 0。
    * */
    default long version() {return 0;}
}
