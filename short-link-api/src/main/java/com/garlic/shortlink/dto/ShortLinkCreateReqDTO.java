package com.garlic.shortlink.dto;

import com.garlic.shortlink.common.validation.Url;
import lombok.Data;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.Size;
import java.io.Serializable;
import java.time.LocalDateTime;

/**
 * 创建短链请求 DTO。
 *
 * <p>请求时不带 userId（由服务端内部模拟登录用户填充，默认 1L）。</p>
 *
 * @author garlic
 */
@Data
public class ShortLinkCreateReqDTO implements Serializable {

    private static final long serialVersionUID = 1L;

    /** 原始长链 */
    @NotBlank(message = "原始 URL 不能为空")
    @Url(message = "URL 不合法")
    private String originalUrl;

    /** 分组ID（可选） */
    private Long groupId;

    /** 过期时间（可选，null 表示永不过期） */
    private LocalDateTime expireTime;

    /** 描述（可选，最长 128 字符） */
    @Size(max = 128, message = "描述最长 128 个字符")
    private String describe;

    /** 用户ID（内部传入，请求时不带，service 默认 1L 模拟登录用户） */
    private Long userId;
}
