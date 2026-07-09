package com.garlic.shortlink.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * 创建短链响应 DTO。
 *
 * @author garlic
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class ShortLinkCreateRespDTO implements Serializable {

    private static final long serialVersionUID = 1L;

    /** 短码 */
    private String shortCode;

    /** 完整短链（= DEFAULT_DOMAIN + shortCode） */
    private String shortUrl;

    /** 原始长链 */
    private String originalUrl;
}
