package com.garlic.shortlink.common.ratelimit;

import com.garlic.shortlink.common.constant.ShortLinkConstants;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Component;

import java.util.Collections;

/**
 * 基于 Redis + Lua 的原子令牌桶限流器（接口级，集群统一）。
 *
 * <p>令牌桶算法核心思想：</p>
 * <ul>
 *   <li>桶有最大容量（capacity），初始满载；</li>
 *   <li>以固定速率（rate/秒）向桶中填充令牌，满则不再填充；</li>
 *   <li>每次请求消耗 1 个令牌，不足则拒绝。</li>
 * </ul>
 *
 * <p>通过 Lua 脚本保证「读取余量 → 计算填充 → 扣减令牌 → 写回」的原子性，
 * 避免多线程/多节点并发下的竞态条件。</p>
 *
 * @author garlic
 */
@Slf4j
@Component
public class TokenBucketRateLimiter {

    /** 令牌桶 Lua 脚本：原子地填充并扣减令牌 */
    private static final String TOKEN_BUCKET_LUA =
            "-- KEYS[1]: 令牌桶 key\n" +
            "-- ARGV[1]: 容量(最大令牌数)\n" +
            "-- ARGV[2]: 每秒填充速率\n" +
            "-- ARGV[3]: 当前时间戳(毫秒)\n" +
            "-- ARGV[4]: 请求消耗令牌数(通常1)\n" +
            "local capacity = tonumber(ARGV[1])\n" +
            "local rate = tonumber(ARGV[2])\n" +
            "local now = tonumber(ARGV[3])\n" +
            "local requested = tonumber(ARGV[4])\n" +
            "local last_time_key = KEYS[1] .. ':last_time'\n" +
            "local tokens_key = KEYS[1] .. ':tokens'\n" +
            "local last_time = tonumber(redis.call('get', last_time_key) or now)\n" +
            "local current_tokens = tonumber(redis.call('get', tokens_key) or capacity)\n" +
            "-- 计算填充的令牌\n" +
            "local delta = math.max(0, now - last_time)\n" +
            "local filled_tokens = math.min(capacity, current_tokens + (delta * rate / 1000))\n" +
            "if filled_tokens >= requested then\n" +
            "    filled_tokens = filled_tokens - requested\n" +
            "    redis.call('setex', tokens_key, 3600, filled_tokens)\n" +
            "    redis.call('setex', last_time_key, 3600, now)\n" +
            "    return 1  -- 允许\n" +
            "else\n" +
            "    redis.call('setex', tokens_key, 3600, filled_tokens)\n" +
            "    redis.call('setex', last_time_key, 3600, now)\n" +
            "    return 0  -- 拒绝\n" +
            "end";

    /** 预编译的 Redis 脚本对象，返回 Long 类型（1=允许, 0=拒绝） */
    private final DefaultRedisScript<Long> script;

    /** 注入 StringRedisTemplate，key/value 均为 String */
    private final StringRedisTemplate stringRedisTemplate;

    /**
     * 构造方法：预编译 Lua 脚本。
     *
     * @param stringRedisTemplate Redis 字符串模板
     */
    public TokenBucketRateLimiter(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
        this.script = new DefaultRedisScript<>();
        this.script.setScriptText(TOKEN_BUCKET_LUA);
        this.script.setResultType(Long.class);
    }

    /**
     * 尝试获取令牌（接口级限流）。
     *
     * <p>完整 Redis key = {@link ShortLinkConstants#RATE_LIMIT_KEY_PREFIX} + "bucket:" + key。</p>
     *
     * @param key           限流标识（如方法全名）
     * @param capacity      令牌桶容量（最大令牌数）
     * @param ratePerSecond 每秒填充速率
     * @return true 表示获取成功（允许请求），false 表示被限流（拒绝请求）
     */
    public boolean tryAcquire(String key, long capacity, long ratePerSecond) {
        String fullKey = ShortLinkConstants.RATE_LIMIT_KEY_PREFIX + "bucket:" + key;
        long now = System.currentTimeMillis();
        Long result = stringRedisTemplate.execute(
                script,
                Collections.singletonList(fullKey),
                String.valueOf(capacity),
                String.valueOf(ratePerSecond),
                String.valueOf(now),
                "1"
        );
        boolean allowed = result != null && result == 1L;
        if (!allowed) {
            log.warn("令牌桶限流拒绝：key={}, capacity={}, rate={}", fullKey, capacity, ratePerSecond);
        }
        return allowed;
    }
}
