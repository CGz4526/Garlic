package com.garlic.shortlink.common.ratelimit;

import com.garlic.shortlink.common.constant.ShortLinkConstants;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Component;

import java.util.Collections;

/**
 * 基于 Redis ZSet 的滑动窗口限流器（用户级/IP级）。
 *
 * <p>滑动窗口算法核心思想：</p>
 * <ul>
 *   <li>以 ZSet 记录窗口内每次请求的时间戳（member=时间戳, score=时间戳）；</li>
 *   <li>每次请求先移除窗口外的过期请求（ZREMRANGEBYSCORE）；</li>
 *   <li>统计当前窗口内请求数（ZCARD），超限则拒绝；</li>
 *   <li>未超限则添加当前请求（ZADD），并刷新过期时间（EXPIRE）。</li>
 * </ul>
 *
 * <p>通过 Lua 脚本保证上述操作的原子性，避免并发下的计数偏差。</p>
 *
 * @author garlic
 */
@Slf4j
@Component
public class SlidingWindowRateLimiter {

    /** 滑动窗口 Lua 脚本：原子地清理过期请求 + 计数 + 添加请求 */
    private static final String SLIDING_WINDOW_LUA =
            "-- KEYS[1]: ZSet key\n" +
            "-- ARGV[1]: 当前时间戳(毫秒)\n" +
            "-- ARGV[2]: 窗口起始时间戳(毫秒)\n" +
            "-- ARGV[3]: 最大请求数\n" +
            "-- ARGV[4]: 过期时间(秒)\n" +
            "local key = KEYS[1]\n" +
            "local now_str = ARGV[1]\n" +
            "local now_num = tonumber(now_str)\n" +
            "local window_start = tonumber(ARGV[2])\n" +
            "local max_requests = tonumber(ARGV[3])\n" +
            "local expire_seconds = tonumber(ARGV[4])\n" +
            "-- 移除窗口外的请求\n" +
            "redis.call('zremrangebyscore', key, 0, window_start)\n" +
            "-- 获取当前窗口内请求数\n" +
            "local count = redis.call('zcard', key)\n" +
            "if count >= max_requests then\n" +
            "    return 0\n" +
            "end\n" +
            "-- 添加当前请求（用时间戳作 member 和 score）\n" +
            "redis.call('zadd', key, now_num, now_str)\n" +
            "redis.call('expire', key, expire_seconds)\n" +
            "return 1";

    /** 预编译的 Redis 脚本对象，返回 Long 类型（1=允许, 0=拒绝） */
    private final DefaultRedisScript<Long> script;

    /** 注入 StringRedisTemplate，key/value 均为 String */
    private final StringRedisTemplate stringRedisTemplate;

    /**
     * 构造方法：预编译 Lua 脚本。
     *
     * @param stringRedisTemplate Redis 字符串模板
     */
    public SlidingWindowRateLimiter(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
        this.script = new DefaultRedisScript<>();
        this.script.setScriptText(SLIDING_WINDOW_LUA);
        this.script.setResultType(Long.class);
    }

    /**
     * 尝试获取请求许可（用户级/IP级限流）。
     *
     * <p>完整 Redis key = {@link ShortLinkConstants#RATE_LIMIT_KEY_PREFIX} + "window:" + key。</p>
     *
     * @param key              限流标识（如 "user:1" 或 "ip:127.0.0.1"）
     * @param windowSizeMillis 窗口大小（毫秒）
     * @param maxRequests      窗口内最大请求数
     * @return true 表示允许请求，false 表示被限流
     */
    public boolean tryAcquire(String key, long windowSizeMillis, long maxRequests) {
        String fullKey = ShortLinkConstants.RATE_LIMIT_KEY_PREFIX + "window:" + key;
        long now = System.currentTimeMillis();
        long windowStart = now - windowSizeMillis;
        long expireSeconds = windowSizeMillis / 1000 + 1;
        Long result = stringRedisTemplate.execute(
                script,
                Collections.singletonList(fullKey),
                String.valueOf(now),
                String.valueOf(windowStart),
                String.valueOf(maxRequests),
                String.valueOf(expireSeconds)
        );
        boolean allowed = result != null && result == 1L;
        if (!allowed) {
            log.warn("滑动窗口限流拒绝：key={}, windowSize={}ms, maxRequests={}", fullKey, windowSizeMillis, maxRequests);
        }
        return allowed;
    }

    /**
     * 查询当前窗口内请求数（不消耗配额）。
     *
     * <p>用于监控/调试场景，先清理过期请求再返回计数。</p>
     *
     * @param key              限流标识
     * @param windowSizeMillis 窗口大小（毫秒）
     * @return 当前窗口内请求数
     */
    public long getCurrentCount(String key, long windowSizeMillis) {
        String fullKey = ShortLinkConstants.RATE_LIMIT_KEY_PREFIX + "window:" + key;
        long now = System.currentTimeMillis();
        long windowStart = now - windowSizeMillis;
        // 先移除窗口外的过期请求
        stringRedisTemplate.opsForZSet().removeRangeByScore(fullKey, 0, windowStart);
        // 统计当前窗口内请求数
        Long count = stringRedisTemplate.opsForZSet().zCard(fullKey);
        return count != null ? count : 0L;
    }
}
