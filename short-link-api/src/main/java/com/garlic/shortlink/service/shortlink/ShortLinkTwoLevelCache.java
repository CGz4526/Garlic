package com.garlic.shortlink.service.shortlink;

import com.garlic.shortlink.common.cache.HotKeyDetector;
import com.garlic.shortlink.common.cache.TwoLevelCache;
import com.garlic.shortlink.common.constant.ShortLinkConstants;
import com.garlic.shortlink.dao.entity.ShortLinkDO;
import com.github.benmanes.caffeine.cache.Cache;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.util.function.Supplier;

/**
 * 短链接二级缓存封装（基于 {@link TwoLevelCache}）。
 *
 * <p>本类置于 short-link-api 模块（可访问 short-link-dao 的 {@link ShortLinkDO}），
 * 对上层业务提供类型安全的短链缓存读写接口，内部委托给通用的
 * {@link TwoLevelCache}（L1 Caffeine + L2 Redis）。</p>
 *
 * <p>缓存配置：</p>
 * <ul>
 *     <li>Redis key 前缀：{@link ShortLinkConstants#CACHE_SHORT_LINK_KEY_PREFIX}（{@code short:link:}）；</li>
 *     <li>Redis TTL 基准：{@link ShortLinkConstants#CACHE_SHORT_LINK_TTL}（3600 秒）；</li>
 *     <li>随机抖动范围：300 秒（防雪崩，最终 TTL ∈ [3600, 3900)）；</li>
 *     <li>序列化：Hutool JSONUtil，缓存中以 JSON 字符串存储。</li>
 * </ul>
 *
 * <p>注意：本类不替代 {@link ShortLinkJumpService}（Task 8），后者将在 Task 13 重构时
 * 切换为调用本类。当前版本仅作为通用二级缓存工具暴露。</p>
 *
 * @author garlic
 */
@Slf4j
@Component
public class ShortLinkTwoLevelCache {

    /** Redis TTL 随机抖动范围（秒，防雪崩） */
    private static final int RANDOM_TTL_RANGE = 300;

    /** 委托的通用二级缓存（K=shortCode, V=ShortLinkDO） */
    private final TwoLevelCache<String, ShortLinkDO> twoLevelCache;

    /**
     * 构造方法：注入底层依赖并构建 {@link TwoLevelCache}。
     *
     * @param shortLinkLocalCache Caffeine 本地缓存（Bean 名 shortLinkLocalCache）
     * @param stringRedisTemplate StringRedisTemplate（L2）
     * @param redissonClient      Redisson 客户端（分布式锁）
     * @param hotKeyDetector      热点 key 探测器
     */
    public ShortLinkTwoLevelCache(
            @Qualifier("shortLinkLocalCache") Cache<String, String> shortLinkLocalCache,
            StringRedisTemplate stringRedisTemplate,
            RedissonClient redissonClient,
            HotKeyDetector hotKeyDetector) {
        this.twoLevelCache = new TwoLevelCache<>(
                shortLinkLocalCache,
                stringRedisTemplate,
                redissonClient,
                ShortLinkConstants.CACHE_SHORT_LINK_KEY_PREFIX,
                ShortLinkConstants.CACHE_SHORT_LINK_TTL,
                RANDOM_TTL_RANGE,
                hotKeyDetector
        );
        log.info("ShortLinkTwoLevelCache 初始化完成：prefix={}, ttl={}s, jitterRange={}s",
                ShortLinkConstants.CACHE_SHORT_LINK_KEY_PREFIX,
                ShortLinkConstants.CACHE_SHORT_LINK_TTL,
                RANDOM_TTL_RANGE);
    }

    /**
     * 查询短链：L1 Caffeine → L2 Redis → DB（带击穿/雪崩防护）。
     *
     * <p>调用方在 {@code dbLoader} 中封装 DB 查询逻辑（含状态/过期校验等业务规则），
     * 缓存未命中时由 {@link TwoLevelCache} 在分布式锁保护下调用。</p>
     *
     * @param shortCode 短码
     * @param dbLoader  DB 加载器（返回 ShortLinkDO，未查到返回 null）
     * @return ShortLinkDO，未命中且 DB 也未查到时返回 null
     */
    public ShortLinkDO getShortLink(String shortCode, Supplier<ShortLinkDO> dbLoader) {
        return twoLevelCache.get(shortCode, dbLoader, ShortLinkDO.class);
    }

    /**
     * 主动写入短链缓存（带随机 TTL，热点 key 同时回填 Caffeine）。
     *
     * @param shortCode  短码
     * @param shortLinkDO 短链实体
     */
    public void putShortLink(String shortCode, ShortLinkDO shortLinkDO) {
        twoLevelCache.put(shortCode, shortLinkDO);
    }

    /**
     * 失效短链缓存（同时清除 L1 与 L2）。
     *
     * @param shortCode 短码
     */
    public void evictShortLink(String shortCode) {
        twoLevelCache.evict(shortCode);
    }
}
