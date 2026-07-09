package com.garlic.shortlink.controller;

import com.garlic.shortlink.common.result.Result;
import com.garlic.shortlink.dto.ShortLinkBatchCreateReqDTO;
import com.garlic.shortlink.dto.ShortLinkBatchCreateRespDTO;
import com.garlic.shortlink.service.shortlink.ShortLinkBatchService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.validation.Valid;

/**
 * 短链批量生成接口。
 *
 * @author garlic
 */
@Slf4j
@RestController
@RequestMapping("/api/short-link")
@RequiredArgsConstructor
public class ShortLinkBatchController {

    /** 注入批量短链生成 Service */
    private final ShortLinkBatchService shortLinkBatchService;

    /**
     * 批量创建短链。
     *
     * @param req 批量请求 DTO（@Valid 触发参数校验：urls 非空且最多 100 条）
     * @return 批量响应 DTO（含成功与失败明细）
     */
    @PostMapping("/batch-create")
    public Result<ShortLinkBatchCreateRespDTO> batchCreate(@Valid @RequestBody ShortLinkBatchCreateReqDTO req) {
        log.info("收到批量创建短链请求：size={}", req.getUrls().size());
        ShortLinkBatchCreateRespDTO resp = shortLinkBatchService.batchCreate(req);
        return Result.success(resp);
    }
}
