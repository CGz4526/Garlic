package com.garlic.shortlink.dto;

import lombok.Data;

import javax.validation.constraints.Size;
import java.io.Serializable;

/**
 * 短链分页查询请求 DTO。
 *
 * <p>查询条件：userId 必填（ShardingSphere 无分片键查询走广播，userId 过滤必要），
 * groupId / shortCode / originalUrl 均为可选过滤条件，shortCode 与 originalUrl 支持模糊查询。</p>
 *
 * @author garlic
 */
@Data
public class ShortLinkPageReqDTO implements Serializable {

    private static final long serialVersionUID = 1L;

    /** 用户ID（必填，默认 1L 模拟登录用户） */
    private Long userId = 1L;

    /** 分组ID（可选） */
    private Long groupId;

    /** 当前页（默认 1） */
    private Long current = 1L;

    /** 每页大小（默认 10，最大 100） */
    @Size(max = 100, message = "每页大小最大 100")
    private Long size = 10L;

    /** 短码（可选，模糊查询） */
    private String shortCode;

    /** 原始长链（可选，模糊查询） */
    private String originalUrl;
}
