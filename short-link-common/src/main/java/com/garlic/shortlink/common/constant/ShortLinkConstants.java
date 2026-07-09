package com.garlic.shortlink.common.constant;

/**
 * 短链接服务公共常量。
 *
 * <p>包含 Redis 缓存 key 前缀、TTL、布隆过滤器名称、Kafka topic、默认域名等。</p>
 *
 * @author garlic
 */
public final class ShortLinkConstants {

    private ShortLinkConstants() {
        throw new UnsupportedOperationException("常量类不可实例化");
    }

    // ==================== Redis 缓存相关 ====================

    /** 短链接 Redis 缓存 key 前缀 */
    public static final String CACHE_SHORT_LINK_KEY_PREFIX = "short:link:";

    /** 短链接缓存 TTL（秒，1 小时） */
    public static final long CACHE_SHORT_LINK_TTL = 3600L;

    /** 布隆过滤器名称 */
    public static final String BLOOM_FILTER_NAME = "short_link:bloom";

    /** 幂等 key 前缀 */
    public static final String IDEMPOTENT_KEY_PREFIX = "short:link:idempotent:";

    /** 限流 key 前缀 */
    public static final String RATE_LIMIT_KEY_PREFIX = "short:link:rate:";

    /** 分布式锁 key 前缀 */
    public static final String LOCK_KEY_PREFIX = "short:link:lock:";

    /** HyperLogLog UV 统计 key 前缀 */
    public static final String HYPERLOGLOG_UV_PREFIX = "short:link:uv:";

    /** IP 去重 Set key 前缀 */
    public static final String IP_SET_PREFIX = "short:link:ip:";

    // ==================== Kafka Topic 相关 ====================

    /** 访问记录上报 topic */
    public static final String TOPIC_ACCESS_RECORD = "short-link-access-record";

    /** 缓存失效广播 topic */
    public static final String TOPIC_CACHE_INVALID = "short-link-cache-invalid";

    // ==================== 其他 ====================

    /** 默认域名 */
    public static final String DEFAULT_DOMAIN = "http://localhost:8001/";
}
