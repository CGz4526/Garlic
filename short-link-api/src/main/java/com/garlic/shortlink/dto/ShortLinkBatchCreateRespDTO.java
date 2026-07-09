package com.garlic.shortlink.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.List;

/**
 * 批量创建短链响应 DTO。
 *
 * <p>包含成功生成的短链列表与失败的原始 URL 列表，
 * 单条失败不影响其他条目，调用方据 failedUrls 重试即可。</p>
 *
 * @author garlic
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class ShortLinkBatchCreateRespDTO implements Serializable {

    private static final long serialVersionUID = 1L;

    /** 成功生成条数 */
    private int successCount;

    /** 失败条数 */
    private int failCount;

    /** 成功生成的短链列表 */
    private List<ShortLinkCreateRespDTO> results;

    /** 失败的原始 URL 列表 */
    private List<String> failedUrls;
}
