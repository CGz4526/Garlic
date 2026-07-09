package com.garlic.shortlink.controller;

import com.garlic.shortlink.common.exception.BizException;
import com.garlic.shortlink.common.exception.ErrorCode;
import com.garlic.shortlink.common.ratelimit.LimitType;
import com.garlic.shortlink.common.ratelimit.RateLimit;
import com.garlic.shortlink.common.util.IpUtils;
import com.garlic.shortlink.service.ratelimit.IpBlacklistService;
import com.garlic.shortlink.service.shortlink.ShortLinkJumpService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletRequest;

/**
 * 短链跳转接口。
 *
 * <p>GET /{code}：根据短码 302 重定向到原始长链。</p>
 *
 * <p>注意：/{code} 路径仅匹配根路径下的单段短码（通常 6~7 位字母数字），
 * 不会与 /api/** 业务接口冲突（后者为多段路径，Spring MVC 优先匹配更具体的映射）。</p>
 *
 * <p>防刷链路（Task 16）：</p>
 * <ul>
 *   <li>IP 黑名单校验：命中黑名单或 1 分钟内访问超阈值（1000 次）直接拒绝；</li>
 *   <li>访问计数：每次请求递增分钟级计数器，超阈值时自动加入黑名单；</li>
 *   <li>与 {@code @RateLimit} 多级限流互补，形成"封禁恶意 IP + 匀速放行正常请求"的双重防护。</li>
 * </ul>
 *
 * @author garlic
 */
@Slf4j
@RestController
@RequiredArgsConstructor
public class ShortLinkJumpController {

    /** 注入短链跳转 Service */
    private final ShortLinkJumpService shortLinkJumpService;

    /** 注入 IP 黑名单服务（接口防刷） */
    private final IpBlacklistService ipBlacklistService;

    /**
     * 短链跳转：302 重定向到原始长链。
     *
     * <p>执行流程：</p>
     * <ol>
     *   <li>获取客户端真实 IP（兼容多层代理）；</li>
     *   <li>IP 黑名单校验：命中黑名单或访问超阈值则抛 {@link ErrorCode#RATE_LIMITED}；</li>
     *   <li>递增当前分钟的访问计数；</li>
     *   <li>调用 Service 执行跳转（布隆过滤器 → 三级缓存）。</li>
     * </ol>
     *
     * @param code    短码
     * @param request HTTP 请求（用于获取客户端 IP）
     * @return 302 重定向响应，Location 头指向原始长链
     */
    @RateLimit(type = LimitType.INTERFACE, capacity = 5000, rate = 5000, message = "跳转接口繁忙，请稍后再试")
    @GetMapping("/{code}")
    public ResponseEntity<Void> jump(@PathVariable("code") String code, HttpServletRequest request) {
        log.info("收到短链跳转请求：code={}", code);

        // IP 黑名单校验（命中黑名单或访问超阈值则直接拒绝）
        String ip = IpUtils.getIpAddr(request);
        if (ipBlacklistService.checkAndBlock(ip)) {
            throw new BizException(ErrorCode.RATE_LIMITED, "访问过于频繁，已被暂时封禁");
        }
        // 递增当前分钟访问计数（用于动态黑名单判定）
        ipBlacklistService.incrementAccess(ip);

        String originalUrl = shortLinkJumpService.jump(code);
        return ResponseEntity.status(302)
                .header("Location", originalUrl)
                .build();
    }
}
