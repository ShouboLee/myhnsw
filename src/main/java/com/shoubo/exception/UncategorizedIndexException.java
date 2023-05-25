package com.shoubo.exception;

/**
 * @author shoubo
 * @date 025 23/5/25
 * @desc 定义在工作线程中发生了一个未分类的异常，即无法明确归类的异常。
 */
public class UncategorizedIndexException extends IndexException{
    private static final long serialVersionUID = 1L;

    /**
     * 构造携带 msg 和异常 cause 的 UncategorizedIndexException
     *
     * @param msg the detail message.
     * @param  cause the cause.
     */
    public UncategorizedIndexException(String msg, Throwable cause) {
        super(msg, cause);
    }
}
