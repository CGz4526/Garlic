package com.garlic.shortlink.dto;

import lombok.Data;

import java.io.Serializable;
import java.time.LocalDateTime;

/**
 * 短链详情响应 DTO（供后续查询接口复用）。
 *
 * @author garlic
 */
@Data
public class ShortLinkRespDTO implements Serializable {

    private static final long serialVersionUID = 1L;

    /** 主键ID */
    private Long id;

    /** 短码 */
    private String shortCode;

    /** 原始长链 */
    private String originalUrl;

    /** 用户ID */
    private Long userId;

    /** 分组ID */
    private Long groupId;

    /** 过期时间，null 表示永不过期 */
    private LocalDateTime expireTime;

    /** 描述 */
    private String describe;

    /** 总访问 PV */
    private Long totalPv;

    /** 总访问 UV */
    private Long totalUv;

    /** 状态：0 正常 1 禁用 */
    private Integer status;

    /** 创建时间 */
    private LocalDateTime createTime;

    /** 更新时间 */
    private LocalDateTime updateTime;

    /** 完整短链（= DEFAULT_DOMAIN + shortCode） */
    private String fullShortUrl;
}
