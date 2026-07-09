package com.garlic.shortlink.common.result;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;

/**
 * 分页响应封装。
 *
 * @author garlic
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class PageResponse<T> implements Serializable {

    private static final long serialVersionUID = 1L;

    /** 当前页 */
    private Long current;

    /** 每页大小 */
    private Long size;

    /** 总记录数 */
    private Long total;

    /** 数据列表 */
    private List<T> records;

    /**
     * 构造空分页结果。
     *
     * @param current 当前页
     * @param size    每页大小
     * @param <T>     数据类型
     * @return 空分页响应
     */
    public static <T> PageResponse<T> empty(Long current, Long size) {
        return new PageResponse<>(current, size, 0L, Collections.emptyList());
    }

    /**
     * 构造分页结果。
     *
     * @param current 当前页
     * @param size    每页大小
     * @param total   总记录数
     * @param records 数据列表
     * @param <T>     数据类型
     * @return 分页响应
     */
    public static <T> PageResponse<T> of(Long current, Long size, Long total, List<T> records) {
        return new PageResponse<>(current, size, total, records);
    }
}
