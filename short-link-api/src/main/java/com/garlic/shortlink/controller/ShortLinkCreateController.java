package com.garlic.shortlink.controller;

import com.garlic.shortlink.common.ratelimit.LimitType;
import com.garlic.shortlink.common.ratelimit.RateLimit;
import com.garlic.shortlink.common.result.Result;
import com.garlic.shortlink.dto.ShortLinkCreateReqDTO;
import com.garlic.shortlink.dto.ShortLinkCreateRespDTO;
import com.garlic.shortlink.service.shortlink.ShortLinkCreateService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.validation.Valid;

/**
 * 短链生成接口。
 *
 * @author garlic
 */
@Slf4j
@RestController
@RequestMapping("/api/short-link")
@RequiredArgsConstructor
public class ShortLinkCreateController {

    /** 注入短链生成 Service */
    private final ShortLinkCreateService shortLinkCreateService;

    /**
     * 创建短链。
     *
     * @param req 创建请求 DTO（@Valid 触发参数校验）
     * @return 创建响应 DTO
     */
    @RateLimit(type = LimitType.USER, maxRequests = 50, windowSize = 1000, message = "生成过于频繁，请稍后再试")
    @PostMapping("/create")
    public Result<ShortLinkCreateRespDTO> create(@Valid @RequestBody ShortLinkCreateReqDTO req) {
        log.info("收到创建短链请求：originalUrl={}", req.getOriginalUrl());
        ShortLinkCreateRespDTO resp = shortLinkCreateService.createShortLink(req);
        return Result.success(resp);
    }
}
