package com.garlic.shortlink.controller;

import com.garlic.shortlink.common.result.PageResponse;
import com.garlic.shortlink.common.result.Result;
import com.garlic.shortlink.dto.ShortLinkPageReqDTO;
import com.garlic.shortlink.dto.ShortLinkRespDTO;
import com.garlic.shortlink.dto.ShortLinkUpdateReqDTO;
import com.garlic.shortlink.service.shortlink.ShortLinkManageService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.validation.Valid;

/**
 * 短链管理接口（CRUD）。
 *
 * <p>与 ShortLinkCreateController 共享同一 base path（/api/short-link），
 * Spring 允许多个 Controller 共享同一 RequestMapping 前缀。</p>
 *
 * @author garlic
 */
@Slf4j
@RestController
@RequestMapping("/api/short-link")
@RequiredArgsConstructor
public class ShortLinkManageController {

    /** 注入短链管理 Service */
    private final ShortLinkManageService shortLinkManageService;

    /**
     * 分页查询短链列表。
     *
     * @param req 分页查询请求（query 参数自动绑定）
     * @return 分页响应
     */
    @GetMapping("/page")
    public Result<PageResponse<ShortLinkRespDTO>> page(ShortLinkPageReqDTO req) {
        log.info("收到短链分页查询请求：userId={}, current={}, size={}",
                req.getUserId(), req.getCurrent(), req.getSize());
        PageResponse<ShortLinkRespDTO> page = shortLinkManageService.pageShortLinks(req);
        return Result.success(page);
    }

    /**
     * 查询短链详情。
     *
     * @param code 短码
     * @return 短链详情
     */
    @GetMapping("/{code}")
    public Result<ShortLinkRespDTO> detail(@PathVariable("code") String code) {
        log.info("收到短链详情查询请求：shortCode={}", code);
        ShortLinkRespDTO resp = shortLinkManageService.getShortLinkDetail(code);
        return Result.success(resp);
    }

    /**
     * 更新短链。
     *
     * @param req 更新请求
     * @return 操作结果
     */
    @PutMapping("/update")
    public Result<Void> update(@Valid @RequestBody ShortLinkUpdateReqDTO req) {
        log.info("收到短链更新请求：shortCode={}", req.getShortCode());
        shortLinkManageService.updateShortLink(req);
        return Result.success();
    }

    /**
     * 逻辑删除短链。
     *
     * @param code 短码
     * @return 操作结果
     */
    @DeleteMapping("/{code}")
    public Result<Void> delete(@PathVariable("code") String code) {
        log.info("收到短链删除请求：shortCode={}", code);
        shortLinkManageService.deleteShortLink(code);
        return Result.success();
    }
}
