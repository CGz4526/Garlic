package com.garlic.shortlink.service.shortlink;

import cn.hutool.json.JSONUtil;
import com.garlic.shortlink.common.bloom.BloomFilterService;
import com.garlic.shortlink.common.constant.ShortLinkConstants;
import com.garlic.shortlink.common.exception.BizException;
import com.garlic.shortlink.common.exception.ErrorCode;
import com.garlic.shortlink.dao.entity.ShortLinkDO;
import com.garlic.shortlink.dao.mapper.ShortLinkMapper;
import com.garlic.shortlink.dto.ShortLinkCreateReqDTO;
import com.garlic.shortlink.dto.ShortLinkCreateRespDTO;
import com.garlic.shortlink.service.shortcode.ShortCodeGenerator;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * 短链生成 Service（含幂等）。
 *
 * <p>幂等设计：相同 longUrl 重复提交返回相同 shortCode，通过 Redis 幂等映射 +
 * Redisson 分布式锁保证并发安全。</p>
 *
 * <p>注：本类原计划放在 short-link-service 模块，但因 DTO 位于 short-link-api 模块，
 * 而 Maven 依赖方向为 api → service（service 不依赖 api），为避免循环依赖，
 * 故将本 Service 置于 short-link-api 模块下 com.garlic.shortlink.service.shortlink 包，
 * 包名与任务要求保持一致，Spring 组件扫描可正常生效。</p>
 *
 * @author garlic
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class ShortLinkCreateService {

    /** 注入短链 Mapper */
    private final ShortLinkMapper shortLinkMapper;

    /** 注入 StringRedisTemplate（缓存 + 幂等映射） */
    private final StringRedisTemplate stringRedisTemplate;

    /** 注入短码生成器 */
    private final ShortCodeGenerator shortCodeGenerator;

    /** 注入 Redisson 客户端（分布式锁） */
    private final RedissonClient redissonClient;

    /** 注入布隆过滤器服务（防缓存穿透，Task 12） */
    private final BloomFilterService bloomFilterService;

    /** 幂等映射 TTL（1 天，秒） */
    private static final long IDEMPOTENT_TTL_SECONDS = 86400L;

    /** 缓存 TTL 随机抖动上限（秒，防雪崩） */
    private static final long CACHE_TTL_JITTER_BOUND = 300L;

    /**
     * 创建短链（含幂等）。
     *
     * <p>流程：</p>
     * <ol>
     *   <li>计算 longUrl 的 hash（组合 userId）作为幂等 key；</li>
     *   <li>查 Redis 幂等映射，命中则直接返回（幂等命中）；</li>
     *   <li>未命中则获取 Redisson 分布式锁（不等待）；</li>
     *   <li>拿到锁后双重检查幂等映射；</li>
     *   <li>生成短码（带冲突检查）；</li>
     *   <li>构建 ShortLinkDO 并写入 DB；</li>
     *   <li>写入 Redis 缓存（带随机抖动防雪崩）；</li>
     *   <li>写入幂等映射（1 天）；</li>
     *   <li>布隆过滤器添加（防缓存穿透）；</li>
     *   <li>释放锁并返回。</li>
     * </ol>
     *
     * @param req 创建请求
     * @return 创建响应
     */
    public ShortLinkCreateRespDTO createShortLink(ShortLinkCreateReqDTO req) {
        // userId 内部传入，默认 1L 模拟登录用户
        Long userId = req.getUserId() != null ? req.getUserId() : 1L;
        String originalUrl = req.getOriginalUrl();

        // 1. 计算幂等 key（longUrl hash + userId）
        String hashKey = ShortLinkConstants.IDEMPOTENT_KEY_PREFIX + userId + ":" + Math.abs(originalUrl.hashCode());

        // 2. 先查 Redis 幂等映射（快速路径）
        String existingShortCode = stringRedisTemplate.opsForValue().get(hashKey);
        if (existingShortCode != null) {
            log.info("幂等命中：hashKey={}, shortCode={}", hashKey, existingShortCode);
            return buildResp(existingShortCode, originalUrl);
        }

        // 3. 未命中，获取 Redisson 分布式锁
        String lockKey = ShortLinkConstants.LOCK_KEY_PREFIX + "create:" + hashKey;
        RLock lock = redissonClient.getLock(lockKey);
        boolean locked = false;
        try {
            // tryLock(0, 10, SECONDS)：不等待，拿不到锁说明有并发请求在处理
            locked = lock.tryLock(0, 10, TimeUnit.SECONDS);
            if (!locked) {
                log.warn("获取分布式锁失败，请勿重复提交：lockKey={}", lockKey);
                throw new BizException(ErrorCode.IDEMPOTENT_REJECT, "请勿重复提交");
            }

            // 4. 双重检查幂等映射
            existingShortCode = stringRedisTemplate.opsForValue().get(hashKey);
            if (existingShortCode != null) {
                log.info("双重检查幂等命中：hashKey={}, shortCode={}", hashKey, existingShortCode);
                return buildResp(existingShortCode, originalUrl);
            }

            // 5. 生成短码（带冲突检查：先用 Redis 查冲突，DB 兜底）
            String shortCode = shortCodeGenerator.generateShortCodeWithConflictCheck(
                    code -> queryShortLinkFromCache(code) != null
            );

            // 6. 构建 ShortLinkDO 对象
            ShortLinkDO shortLinkDO = new ShortLinkDO();
            shortLinkDO.setShortCode(shortCode);
            shortLinkDO.setOriginalUrl(originalUrl);
            shortLinkDO.setUserId(userId);
            shortLinkDO.setGroupId(req.getGroupId());
            shortLinkDO.setExpireTime(req.getExpireTime());
            shortLinkDO.setDescribe(req.getDescribe());
            shortLinkDO.setTotalPv(0L);
            shortLinkDO.setTotalUv(0L);
            shortLinkDO.setStatus(0);
            shortLinkDO.setDelFlag(0);

            // 7. 写入 DB（ShardingSphere 自动路由）
            shortLinkMapper.insert(shortLinkDO);
            log.info("短链写入 DB 成功：id={}, shortCode={}", shortLinkDO.getId(), shortCode);

            // 8. 写入 Redis 缓存（TTL + 随机抖动 0~300 秒防雪崩）
            writeShortLinkToCache(shortLinkDO);

            // 9. 写入幂等映射（1 天）
            stringRedisTemplate.opsForValue().set(hashKey, shortCode, IDEMPOTENT_TTL_SECONDS, TimeUnit.SECONDS);

            // 10. 布隆过滤器添加（防缓存穿透，Task 12）
            bloomFilterService.add(shortCode);
            log.info("布隆过滤器添加 shortCode 成功：shortCode={}", shortCode);

            return buildResp(shortCode, originalUrl);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("获取分布式锁被中断：lockKey={}", lockKey, e);
            throw new BizException(ErrorCode.SYSTEM_ERROR, "创建短链被中断");
        } finally {
            // 11. 释放锁
            if (locked && lock.isHeldByCurrentThread()) {
                lock.unlock();
            }
        }
    }

    /**
     * 从 Redis 缓存查询短链。
     *
     * <p>从 Redis 读取 cacheKey，使用 Hutool JSONUtil 反序列化为 ShortLinkDO。</p>
     *
     * @param shortCode 短码
     * @return ShortLinkDO，未命中或反序列化失败返回 null
     */
    public ShortLinkDO queryShortLinkFromCache(String shortCode) {
        String cacheKey = ShortLinkConstants.CACHE_SHORT_LINK_KEY_PREFIX + shortCode;
        String json = stringRedisTemplate.opsForValue().get(cacheKey);
        if (json == null) {
            return null;
        }
        try {
            return JSONUtil.toBean(json, ShortLinkDO.class);
        } catch (Exception e) {
            log.error("反序列化短链缓存失败：shortCode={}, json={}", shortCode, json, e);
            return null;
        }
    }

    /**
     * 写入短链到 Redis 缓存，TTL = CACHE_SHORT_LINK_TTL + 随机抖动(0~300秒) 防雪崩。
     *
     * @param shortLinkDO 短链 DO
     */
    private void writeShortLinkToCache(ShortLinkDO shortLinkDO) {
        String cacheKey = ShortLinkConstants.CACHE_SHORT_LINK_KEY_PREFIX + shortLinkDO.getShortCode();
        String json = JSONUtil.toJsonStr(shortLinkDO);
        long ttl = ShortLinkConstants.CACHE_SHORT_LINK_TTL
                + ThreadLocalRandom.current().nextLong(CACHE_TTL_JITTER_BOUND);
        stringRedisTemplate.opsForValue().set(cacheKey, json, ttl, TimeUnit.SECONDS);
    }

    /**
     * 构建创建响应 DTO。
     *
     * @param shortCode   短码
     * @param originalUrl 原始长链
     * @return 响应 DTO
     */
    private ShortLinkCreateRespDTO buildResp(String shortCode, String originalUrl) {
        String shortUrl = ShortLinkConstants.DEFAULT_DOMAIN + shortCode;
        return new ShortLinkCreateRespDTO(shortCode, shortUrl, originalUrl);
    }
}
