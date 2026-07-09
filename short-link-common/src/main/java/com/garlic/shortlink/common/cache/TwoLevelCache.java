package com.garlic.shortlink.common.cache;

import cn.hutool.json.JSONUtil;
import com.garlic.shortlink.common.constant.ShortLinkConstants;
import com.github.benmanes.caffeine.cache.Cache;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * 二级缓存抽象：L1 Caffeine（本地）+ L2 Redis（分布式）。
 *
 * <p>封装通用查询链路：L1 命中 → L2 命中（按需回填 L1）→ 分布式锁防击穿 → DB 加载 → 回填 L2/L1。
 * 配合 {@link HotKeyDetector} 控制热点 key 才进入 L1 Caffeine，避免本地缓存被冷数据撑爆。</p>
 *
 * <p>防护策略：</p>
 * <ul>
 *     <li><b>缓存击穿</b>：Redisson 分布式锁 + 双重检查，未拿到锁的请求短暂自旋后重试 Redis；</li>
 *     <li><b>缓存雪崩</b>：回填 Redis 时 TTL = {@link #redisTtlSeconds} + 随机抖动（0 ~ {@link #randomTtlRange}）；</li>
 *     <li><b>缓存穿透</b>：由调用方在 {@code dbLoader} 中处理（如返回 null 或缓存空对象）。</li>
 * </ul>
 *
 * <p>序列化方案：L1 与 L2 均存储 JSON 字符串，使用 Hutool {@link JSONUtil} 完成对象 ↔ JSON 转换。</p>
 *
 * @param <K> 缓存 key 类型
 * @param <V> 缓存值类型
 * @author garlic
 */
@Slf4j
public class TwoLevelCache<K, V> {

    /** L1 本地缓存（Caffeine，仅存热点 key 的 JSON） */
    private final Cache<String, String> caffeineCache;

    /** L2 分布式缓存（StringRedisTemplate，存 JSON 字符串） */
    private final StringRedisTemplate redisTemplate;

    /** Redisson 客户端（分布式锁，防缓存击穿） */
    private final RedissonClient redissonClient;

    /** Redis key 前缀（如 {@code short:link:}） */
    private final String redisKeyPrefix;

    /** Redis 过期时间基准（秒） */
    private final long redisTtlSeconds;

    /** 随机抖动范围（秒，防雪崩） */
    private final int randomTtlRange;

    /** 热点 key 探测器（决定是否回填 Caffeine） */
    private final HotKeyDetector hotKeyDetector;

    /** 击穿防护：未拿到锁时的短暂休眠（毫秒） */
    private static final long LOCK_RETRY_SLEEP_MS = 50L;

    /** 击穿防护：分布式锁持有时间（秒） */
    private static final long LOCK_HOLD_SECONDS = 5L;

    /** 击穿防护：tryLock 等待时间（0 表示不等待） */
    private static final long LOCK_WAIT_SECONDS = 0L;

    /**
     * 构造方法：注入所有依赖。
     *
     * @param caffeineCache   L1 Caffeine 缓存
     * @param redisTemplate   L2 StringRedisTemplate
     * @param redissonClient  Redisson 客户端
     * @param redisKeyPrefix  Redis key 前缀
     * @param redisTtlSeconds Redis 过期时间基准（秒）
     * @param randomTtlRange  随机抖动范围（秒）
     * @param hotKeyDetector  热点 key 探测器
     */
    public TwoLevelCache(Cache<String, String> caffeineCache,
                         StringRedisTemplate redisTemplate,
                         RedissonClient redissonClient,
                         String redisKeyPrefix,
                         long redisTtlSeconds,
                         int randomTtlRange,
                         HotKeyDetector hotKeyDetector) {
        this.caffeineCache = caffeineCache;
        this.redisTemplate = redisTemplate;
        this.redissonClient = redissonClient;
        this.redisKeyPrefix = redisKeyPrefix;
        this.redisTtlSeconds = redisTtlSeconds;
        this.randomTtlRange = randomTtlRange;
        this.hotKeyDetector = hotKeyDetector;
    }

    /**
     * 查询缓存（L1 → L2 → DB，含击穿防护与雪崩防护）。
     *
     * <p>查询链路：</p>
     * <ol>
     *     <li>记录一次访问到 {@link HotKeyDetector}（用于热点判定）；</li>
     *     <li>L1 Caffeine 命中 → 反序列化返回（仅热点 key 会存在于 L1）；</li>
     *     <li>L2 Redis 命中 → 反序列化返回，若为热点 key 则回填 L1；</li>
     *     <li>缓存未命中 → 加分布式锁双重检查 Redis，仍未命中则调用 dbLoader 查 DB；</li>
     *     <li>回填 L2（带随机 TTL）与 L1（仅热点 key）。</li>
     * </ol>
     *
     * @param key      业务 key
     * @param dbLoader DB 加载器（缓存未命中时调用）
     * @param clazz    返回值类型（用于 JSON 反序列化）
     * @return 缓存或 DB 加载的值，DB 也未命中时返回 null
     */
    public V get(K key, Supplier<V> dbLoader, Class<V> clazz) {
        String cacheKey = key == null ? null : key.toString();
        if (cacheKey == null || cacheKey.isEmpty()) {
            return null;
        }

        // 记录访问，用于热点探测
        hotKeyDetector.recordAccess(cacheKey);

        // ===== L1 Caffeine 本地缓存（仅热点 key 才会在此） =====
        String localJson = caffeineCache.getIfPresent(cacheKey);
        if (localJson != null) {
            V hit = parseJson(localJson, clazz, cacheKey);
            if (hit != null) {
                log.debug("L1 Caffeine 命中：cacheKey={}", cacheKey);
                return hit;
            }
            // 反序列化失败，清理脏数据
            caffeineCache.invalidate(cacheKey);
        }

        // ===== L2 Redis 分布式缓存 =====
        String redisKey = redisKeyPrefix + cacheKey;
        Supplier<String> redisGetter = () -> redisTemplate.opsForValue().get(redisKey);
        String redisJson = redisGetter.get();
        if (redisJson != null) {
            V hit = parseJson(redisJson, clazz, cacheKey);
            if (hit != null) {
                log.debug("L2 Redis 命中：cacheKey={}", cacheKey);
                backfillCaffeineIfHot(cacheKey, redisJson);
                return hit;
            }
            // 反序列化失败，清理脏数据
            redisTemplate.delete(redisKey);
        }

        // ===== 缓存未命中，加分布式锁防击穿 =====
        return getWithLock(cacheKey, redisGetter, dbLoader, clazz);
    }

    /**
     * 缓存击穿防护：分布式锁 + 双重检查。
     *
     * <p>流程：</p>
     * <ol>
     *     <li>{@code tryLock(0, 5, SECONDS)}：不等待，持有 5 秒；</li>
     *     <li>拿到锁 → 双重检查 Redis，仍未命中则查 DB 并回填；</li>
     *     <li>拿不到锁 → {@code sleep(50ms)} 后重试 Redis，仍未命中则直接查 DB（兜底）。</li>
     * </ol>
     *
     * @param cacheKey    缓存 key（已 toString）
     * @param redisGetter Redis 取值函数（封装 redisKey 细节）
     * @param dbLoader    DB 加载器
     * @param clazz       返回值类型
     * @return 加载到的值，可能为 null
     */
    private V getWithLock(String cacheKey,
                          Supplier<String> redisGetter,
                          Supplier<V> dbLoader,
                          Class<V> clazz) {
        String lockKey = ShortLinkConstants.LOCK_KEY_PREFIX + "cache:" + cacheKey;
        RLock lock = redissonClient.getLock(lockKey);
        boolean locked = false;
        try {
            locked = lock.tryLock(LOCK_WAIT_SECONDS, LOCK_HOLD_SECONDS, TimeUnit.SECONDS);
            if (locked) {
                // 拿到锁：双重检查 Redis（可能已被其他请求重建）
                String redisJson = redisGetter.get();
                if (redisJson != null) {
                    V hit = parseJson(redisJson, clazz, cacheKey);
                    if (hit != null) {
                        log.debug("双重检查 Redis 命中：cacheKey={}", cacheKey);
                        backfillCaffeineIfHot(cacheKey, redisJson);
                        return hit;
                    }
                }
                // 仍未命中，查 DB 并回填缓存
                V value = dbLoader.get();
                if (value != null) {
                    putByCacheKey(cacheKey, value);
                }
                return value;
            } else {
                // 拿不到锁：短暂 sleep(50ms) 后重试一次 Redis 查询，仍未命中则查 DB 兜底
                sleepQuietly(LOCK_RETRY_SLEEP_MS);
                String redisJson = redisGetter.get();
                if (redisJson != null) {
                    V hit = parseJson(redisJson, clazz, cacheKey);
                    if (hit != null) {
                        log.debug("等待后 Redis 重试命中：cacheKey={}", cacheKey);
                        backfillCaffeineIfHot(cacheKey, redisJson);
                        return hit;
                    }
                }
                // 仍未命中，直接查 DB（兜底，不再加锁避免阻塞）
                log.debug("等待锁失败后直接查 DB：cacheKey={}", cacheKey);
                return dbLoader.get();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("获取分布式锁被中断：lockKey={}", lockKey, e);
            // 中断时直接查 DB 兜底
            return dbLoader.get();
        } finally {
            if (locked && lock.isHeldByCurrentThread()) {
                try {
                    lock.unlock();
                } catch (IllegalMonitorStateException e) {
                    // 锁已自动释放（持有时间到期），忽略
                    log.warn("分布式锁已自动释放：lockKey={}", lockKey);
                }
            }
        }
    }

    /**
     * 主动写入缓存（带随机 TTL，仅热点 key 写入 L1）。
     *
     * @param key   业务 key
     * @param value 缓存值
     */
    public void put(K key, V value) {
        if (key == null || value == null) {
            return;
        }
        putByCacheKey(key.toString(), value);
    }

    /**
     * 按 cacheKey 写入缓存（内部复用，避免在 getWithLock 中再次 toString 转换）。
     *
     * @param cacheKey 缓存 key（已 toString）
     * @param value    缓存值
     */
    private void putByCacheKey(String cacheKey, V value) {
        if (cacheKey == null || cacheKey.isEmpty() || value == null) {
            return;
        }
        String redisKey = redisKeyPrefix + cacheKey;
        String json = JSONUtil.toJsonStr(value);
        long ttl = computeTtlWithJitter();
        redisTemplate.opsForValue().set(redisKey, json, ttl, TimeUnit.SECONDS);
        backfillCaffeineIfHot(cacheKey, json);
        log.debug("缓存写入完成：cacheKey={}, ttl={}s", cacheKey, ttl);
    }

    /**
     * 失效缓存（同时清除 L1 与 L2）。
     *
     * @param key 业务 key
     */
    public void evict(K key) {
        if (key == null) {
            return;
        }
        String cacheKey = key.toString();
        String redisKey = redisKeyPrefix + cacheKey;
        redisTemplate.delete(redisKey);
        caffeineCache.invalidate(cacheKey);
        log.debug("缓存失效完成：cacheKey={}", cacheKey);
    }

    /**
     * 计算带随机抖动的 TTL（防雪崩）。
     *
     * <p>TTL = {@link #redisTtlSeconds} + {@link ThreadLocalRandom#nextInt(int)}({@link #randomTtlRange})。
     * 当 {@link #randomTtlRange} ≤ 0 时，不添加抖动。</p>
     *
     * @return 带 jitter 的 TTL（秒）
     */
    private long computeTtlWithJitter() {
        if (randomTtlRange <= 0) {
            return redisTtlSeconds;
        }
        return redisTtlSeconds + ThreadLocalRandom.current().nextInt(randomTtlRange);
    }

    /**
     * 若 cacheKey 为热点 key，则将 JSON 回填到 Caffeine（L1）。
     *
     * @param cacheKey 缓存 key
     * @param json     JSON 字符串
     */
    private void backfillCaffeineIfHot(String cacheKey, String json) {
        if (hotKeyDetector.isHotKey(cacheKey)) {
            caffeineCache.put(cacheKey, json);
            log.debug("热点 key 回填 Caffeine：cacheKey={}", cacheKey);
        }
    }

    /**
     * 反序列化 JSON 为指定类型。
     *
     * @param json     JSON 字符串
     * @param clazz    目标类型
     * @param cacheKey 缓存 key（仅用于日志）
     * @return 反序列化后的对象，失败返回 null
     */
    private V parseJson(String json, Class<V> clazz, String cacheKey) {
        if (json == null || json.isEmpty()) {
            return null;
        }
        try {
            return JSONUtil.toBean(json, clazz);
        } catch (Exception e) {
            log.error("反序列化缓存失败：cacheKey={}, json={}", cacheKey, json, e);
            return null;
        }
    }

    /**
     * 安静地休眠（被中断时恢复中断标志位，不抛出 InterruptedException）。
     *
     * @param millis 休眠毫秒数
     */
    private void sleepQuietly(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
