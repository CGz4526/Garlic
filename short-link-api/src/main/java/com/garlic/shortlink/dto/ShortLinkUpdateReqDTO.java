package com.garlic.shortlink.dto;

import lombok.Data;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.Size;
import java.io.Serializable;
import java.time.LocalDateTime;

/**
 * 短链更新请求 DTO。
 *
 * <p>按 shortCode 定位记录（shortCode 为分片键，精确路由），其余字段可选更新。</p>
 *
 * @author garlic
 */
@Data
public class ShortLinkUpdateReqDTO implements Serializable {

    private static final long serialVersionUID = 1L;

    /** 短码（必填，作为更新定位条件） */
    @NotBlank(message = "短码不能为空")
    private String shortCode;

    /** 原始长链（可选） */
    private String originalUrl;

    /** 分组ID（可选） */
    private Long groupId;

    /** 过期时间（可选，null 表示永不过期） */
    private LocalDateTime expireTime;

    /** 描述（可选） */
    @Size(max = 128, message = "描述最长 128 个字符")
    private String describe;

    /** 状态（可选，0 正常 1 禁用） */
    private Integer status;
}
