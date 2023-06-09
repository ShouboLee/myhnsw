package com.shoubo.model.bo;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

import java.io.Serializable;
import java.util.Comparator;

/**
 * @author shoubo
 * @date 026 23/5/26
 * @desc 最近邻搜索的结果实体类
 */
@AllArgsConstructor
@Getter
@ToString
@EqualsAndHashCode
public class SearchResultBO<TItem, TDistance>
        implements Comparable<SearchResultBO<TItem, TDistance>>, Serializable {
    private static final long serialVersionUID = 1L;

    /**
     * 搜索查询的距离
     */
    private final TDistance distance;

    /**
     * 返回的item
     */
    private final TItem item;

    /**
     * 用于比较距离的比较器
     */
    private final Comparator<TDistance> distanceComparator;

    @Override
    public int compareTo(SearchResultBO<TItem, TDistance> o) {
        return distanceComparator.compare(distance, o.distance);
    }

    /**
     * 静态方法，用于创建一个具有可比较距离的 SearchResultBO 实例
     * @param item item
     * @param distance 距离
     * @return new一个SearchResultBO对象
     * @param <TItem> item返回类型
     * @param <TDistance> 距离的返回类型
     */
    public static <TItem, TDistance extends Comparable<TDistance>> SearchResultBO<TItem, TDistance> create(TItem item, TDistance distance) {
        return new SearchResultBO<>(distance, item, Comparator.naturalOrder());
    }

}
