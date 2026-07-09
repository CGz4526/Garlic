package com.garlic.shortlink.dto;

import lombok.Data;

import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.Size;
import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.List;

/**
 * 批量创建短链请求 DTO。
 *
 * <p>批量共享同一 groupId / expireTime / describe，每个 URL 单独生成一条短链。
 * 单次最多 100 条，防止单批任务过大拖垮线程池。</p>
 *
 * @author garlic
 */
@Data
public class ShortLinkBatchCreateReqDTO implements Serializable {

    private static final long serialVersionUID = 1L;

    /** 原始 URL 列表（最多 100 条） */
    @NotEmpty(message = "URL 列表不能为空")
    @Size(max = 100, message = "单批最多 100 条 URL")
    private List<String> urls;

    /** 分组ID（可选，整批共享） */
    private Long groupId;

    /** 过期时间（可选，整批共享，null 表示永不过期） */
    private LocalDateTime expireTime;

    /** 描述（可选，整批共享，最长 128 字符） */
    @Size(max = 128, message = "描述最长 128 个字符")
    private String describe;
}
