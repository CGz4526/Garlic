package com.garlic.shortlink.service.ratelimit;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.TimeUnit;

/**
 * IP 黑名单服务（接口防刷核心组件）。
 *
 * <p>基于 Redis Set + 分钟级访问计数器实现动态 IP 黑名单：</p>
 * <ul>
 *   <li>命中黑名单的 IP 直接拒绝访问（{@link #checkAndBlock(String)} 返回 true）；</li>
 *   <li>1 分钟内访问次数超过阈值（默认 1000）的 IP 自动加入黑名单；</li>
 *   <li>黑名单过期时间 1 小时，过期后自动解封。</li>
 * </ul>
 *
 * <p>设计说明：</p>
 * <ul>
 *   <li>黑名单使用 Redis Set 存储，SISMEMBER 时间复杂度 O(1)；</li>
 *   <li>访问计数器按"分钟"分桶（key 含 yyyyMMddHHmm），过期时间 70 秒（略大于 1 分钟），
 *       避免跨分钟边界时计数被过早清理；</li>
 *   <li>与 {@code @RateLimit} 注解的多级限流互补：限流是"匀速放行"，黑名单是"封禁恶意 IP"。</li>
 * </ul>
 *
 * @author garlic
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class IpBlacklistService {

    /** 注入 StringRedisTemplate（黑名单 Set + 访问计数器） */
    private final StringRedisTemplate stringRedisTemplate;

    /** IP 黑名单 Redis Set key */
    private static final String IP_BLACKLIST_KEY = "short:link:ip:blacklist";

    /** IP 访问计数器 key 前缀 */
    private static final String IP_ACCESS_COUNTER_PREFIX = "short:link:ip:counter:";

    /** 1 分钟内访问次数阈值，超过则加入黑名单 */
    private static final long THRESHOLD = 1000L;

    /** 黑名单过期时间（秒，1 小时） */
    private static final long BLACKLIST_TTL = 3600L;

    /** 访问计数器过期时间（秒，略大于 1 分钟） */
    private static final long COUNTER_TTL = 70L;

    /** 分钟级 key 格式化器：yyyyMMddHHmm */
    private static final DateTimeFormatter MINUTE_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMddHHmm");

    /**
     * 检查 IP 是否应被阻止（命中黑名单或超过访问阈值）。
     *
     * <p>逻辑：</p>
     * <ol>
     *   <li>先查黑名单 Set，若在黑名单中直接返回 true；</li>
     *   <li>查当前分钟的访问计数，若 >= 阈值则加入黑名单并返回 true；</li>
     *   <li>否则返回 false。</li>
     * </ol>
     *
     * @param ip 客户端 IP
     * @return true 表示应被阻止（命中黑名单或超阈值），false 表示放行
     */
    public boolean checkAndBlock(String ip) {
        if (ip == null || ip.isEmpty()) {
            return false;
        }

        // 1. 先查黑名单：SISMEMBER IP_BLACKLIST_KEY ip
        Boolean inBlacklist = stringRedisTemplate.opsForSet().isMember(IP_BLACKLIST_KEY, ip);
        if (Boolean.TRUE.equals(inBlacklist)) {
            log.warn("IP 命中黑名单，拒绝访问：ip={}", ip);
            return true;
        }

        // 2. 查当前分钟访问计数：GET IP_ACCESS_COUNTER_PREFIX + ip + ":" + currentMinuteKey
        String counterKey = buildCounterKey(ip);
        String countStr = stringRedisTemplate.opsForValue().get(counterKey);
        long count = countStr != null ? Long.parseLong(countStr) : 0L;
        if (count >= THRESHOLD) {
            // 计数 >= 阈值，加入黑名单
            addToBlacklist(ip);
            log.warn("IP 访问过于频繁，已加入黑名单：ip={}, count={}, threshold={}",
                    ip, count, THRESHOLD);
            return true;
        }

        // 3. 放行
        return false;
    }

    /**
     * 递增 IP 的当前分钟访问计数。
     *
     * <p>INCR 计数器，并设置 70 秒过期（略大于 1 分钟，避免跨分钟边界时被过早清理）。
     * 仅在计数为 1 时设置过期时间是常见优化，但此处为简化实现统一设置。
     * 注意：INCR 后再 EXPIRE 非原子操作，极端情况下计数器可能未设置过期时间导致残留，
     * 可通过 Lua 脚本优化（当前实现满足防刷需求）。</p>
     *
     * @param ip 客户端 IP
     */
    public void incrementAccess(String ip) {
        if (ip == null || ip.isEmpty()) {
            return;
        }
        String counterKey = buildCounterKey(ip);
        Long count = stringRedisTemplate.opsForValue().increment(counterKey);
        // 首次访问时设置过期时间，避免 key 永久残留
        if (count != null && count == 1L) {
            stringRedisTemplate.expire(counterKey, COUNTER_TTL, TimeUnit.SECONDS);
        }
    }

    /**
     * 判断 IP 是否在黑名单中。
     *
     * @param ip 客户端 IP
     * @return true 表示在黑名单中
     */
    public boolean isBlacklisted(String ip) {
        if (ip == null || ip.isEmpty()) {
            return false;
        }
        Boolean inBlacklist = stringRedisTemplate.opsForSet().isMember(IP_BLACKLIST_KEY, ip);
        return Boolean.TRUE.equals(inBlacklist);
    }

    /**
     * 将 IP 加入黑名单。
     *
     * <p>SADD 加入 Redis Set，并刷新黑名单整体过期时间。</p>
     *
     * @param ip 客户端 IP
     */
    public void addToBlacklist(String ip) {
        if (ip == null || ip.isEmpty()) {
            return;
        }
        stringRedisTemplate.opsForSet().add(IP_BLACKLIST_KEY, ip);
        stringRedisTemplate.expire(IP_BLACKLIST_KEY, BLACKLIST_TTL, TimeUnit.SECONDS);
    }

    /**
     * 构建分钟级访问计数器 key。
     *
     * <p>格式：short:link:ip:counter:{ip}:{yyyyMMddHHmm}</p>
     *
     * @param ip 客户端 IP
     * @return 计数器 key
     */
    private String buildCounterKey(String ip) {
        String currentMinuteKey = LocalDateTime.now().format(MINUTE_FORMATTER);
        return IP_ACCESS_COUNTER_PREFIX + ip + ":" + currentMinuteKey;
    }
}
