package com.shoubo.listener;

/**
 * Author: shoubo
 * Date: 2023/5/24
 * Desc: Callback interface for reporting on the progress of an index operation.
 * 定义了一个接口 ProgressListener，用于在索引操作过程中报告进度
 */
@FunctionalInterface
public interface ProgressListener {
    void updateProgress(int workDone, int max);
}
