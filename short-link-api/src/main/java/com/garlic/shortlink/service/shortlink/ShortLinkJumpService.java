package com.garlic.shortlink.service.shortlink;

import cn.hutool.crypto.SecureUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.alibaba.csp.sentinel.annotation.SentinelResource;
import com.alibaba.csp.sentinel.slots.block.BlockException;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.garlic.shortlink.common.bloom.BloomFilterService;
import com.garlic.shortlink.common.constant.ShortLinkConstants;
import com.garlic.shortlink.common.exception.BizException;
import com.garlic.shortlink.common.exception.ErrorCode;
import com.garlic.shortlink.common.util.IpUtils;
import com.garlic.shortlink.common.util.UserAgentUtils;
import com.garlic.shortlink.dao.entity.ShortLinkDO;
import com.garlic.shortlink.dao.mapper.ShortLinkMapper;
import com.garlic.shortlink.service.mq.AccessRecordQueue;
import com.github.benmanes.caffeine.cache.Cache;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

import javax.servlet.http.HttpServletRequest;
import java.time.LocalDateTime;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * 短链跳转 Service（三级缓存链路）。
 *
 * <p>查询链路：L1 Caffeine 本地缓存 → L2 Redis 分布式缓存 → DB，
 * 配合 Redisson 分布式锁防止缓存击穿（基础版，Task 11 封装通用工具）。</p>
 *
 * <p>注：本类置于 short-link-api 模块下 com.garlic.shortlink.service.shortlink 包，
 * 与 ShortLinkCreateService 保持一致，Spring 组件扫描可正常生效。</p>
 *
 * @author garlic
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class ShortLinkJumpService {

    /** 注入 StringRedisTemplate（L2 分布式缓存） */
    private final StringRedisTemplate stringRedisTemplate;

    /** 注入布隆过滤器服务（前置校验，防缓存穿透） */
    private final BloomFilterService bloomFilterService;

    /** 注入短链 Mapper（DB 查询） */
    private final ShortLinkMapper shortLinkMapper;

    /** 注入 Caffeine 本地缓存（L1，Bean 名 shortLinkLocalCache） */
    @Qualifier("shortLinkLocalCache")
    private final Cache<String, String> shortLinkLocalCache;

    /** 注入 Redisson 客户端（分布式锁，防缓存击穿） */
    private final RedissonClient redissonClient;

    /** 注入访问记录本地队列（异步上报到 Kafka，不影响跳转 RT） */
    private final AccessRecordQueue accessRecordQueue;

    /** 缓存 TTL 随机抖动上限（秒，防雪崩） */
    private static final int CACHE_TTL_JITTER_BOUND = 300;

    /** 击穿防护：未拿到锁时的短暂休眠（毫秒） */
    private static final long LOCK_RETRY_SLEEP_MS = 50L;

    /** 击穿防护：分布式锁持有时间（秒） */
    private static final long LOCK_HOLD_SECONDS = 5L;

    /**
     * 短链跳转：根据短码查询原始 URL。
     *
     * <p>三级缓存查询链路：</p>
     * <ol>
     *   <li>L1 Caffeine 本地缓存命中 → 直接返回；</li>
     *   <li>L2 Redis 分布式缓存命中 → 回填 Caffeine 后返回；</li>
     *   <li>DB 查询（加分布式锁防击穿）→ 回填 Redis + Caffeine 后返回。</li>
     * </ol>
     *
     * @param shortCode 短码
     * @return 原始长链
     */
    @SentinelResource(value = "jumpResource", blockHandler = "jumpBlockHandler", fallback = "jumpFallback")
    public String jump(String shortCode) {
        // 布隆过滤器前置校验，防止缓存穿透
        // 注意：布隆过滤器有 0.1% 误判率，不存在的 shortCode 可能误判为存在，
        // 但存在的 shortCode 不会误判为不存在，所以只能防"确定不存在"的请求
        if (!bloomFilterService.contains(shortCode)) {
            log.warn("布隆过滤器校验不通过，短链一定不存在：shortCode={}", shortCode);
            throw new BizException(ErrorCode.SHORT_LINK_NOT_FOUND);
        }

        // ===== L1 Caffeine 本地缓存 =====
        // cacheKey = shortCode（本地缓存只存热点 key，后续 Task 11 增强热点探测）
        String localJson = shortLinkLocalCache.getIfPresent(shortCode);
        if (localJson != null) {
            log.info("L1 Caffeine 命中：shortCode={}", shortCode);
            ShortLinkDO hit = parseShortLink(localJson, shortCode);
            if (hit != null) {
                return hit.getOriginalUrl();
            }
        }

        // ===== L2 Redis 分布式缓存 =====
        String redisKey = ShortLinkConstants.CACHE_SHORT_LINK_KEY_PREFIX + shortCode;
        String redisJson = stringRedisTemplate.opsForValue().get(redisKey);
        if (redisJson != null) {
            log.info("L2 Redis 命中：shortCode={}", shortCode);
            ShortLinkDO hit = parseShortLink(redisJson, shortCode);
            if (hit != null) {
                // 回填 Caffeine
                shortLinkLocalCache.put(shortCode, redisJson);
                return hit.getOriginalUrl();
            }
        }

        // ===== 缓存未命中，加分布式锁防击穿（基础版） =====
        // lockKey = LOCK_KEY_PREFIX + "jump:" + shortCode
        String lockKey = ShortLinkConstants.LOCK_KEY_PREFIX + "jump:" + shortCode;
        RLock lock = redissonClient.getLock(lockKey);
        boolean locked = false;
        try {
            // tryLock(0, 5, SECONDS)：不等待，持有 5 秒
            locked = lock.tryLock(0, LOCK_HOLD_SECONDS, TimeUnit.SECONDS);
            if (locked) {
                // 拿到锁：双重检查 Redis（可能已被其他请求重建）
                redisJson = stringRedisTemplate.opsForValue().get(redisKey);
                if (redisJson != null) {
                    log.info("双重检查 Redis 命中：shortCode={}", shortCode);
                    ShortLinkDO hit = parseShortLink(redisJson, shortCode);
                    if (hit != null) {
                        shortLinkLocalCache.put(shortCode, redisJson);
                        return hit.getOriginalUrl();
                    }
                }
                // 仍未命中，查 DB 并回填缓存
                return loadFromDbAndBackfill(shortCode, redisKey);
            } else {
                // 拿不到锁：短暂 sleep(50ms) 后重试一次 Redis 查询，仍未命中则查 DB
                sleepQuietly(LOCK_RETRY_SLEEP_MS);
                redisJson = stringRedisTemplate.opsForValue().get(redisKey);
                if (redisJson != null) {
                    log.info("等待后 Redis 重试命中：shortCode={}", shortCode);
                    ShortLinkDO hit = parseShortLink(redisJson, shortCode);
                    if (hit != null) {
                        shortLinkLocalCache.put(shortCode, redisJson);
                        return hit.getOriginalUrl();
                    }
                }
                // 仍未命中，直接查 DB
                return loadFromDbAndBackfill(shortCode, redisKey);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("获取分布式锁被中断：lockKey={}", lockKey, e);
            throw new BizException(ErrorCode.SYSTEM_ERROR, "短链跳转被中断");
        } finally {
            if (locked && lock.isHeldByCurrentThread()) {
                lock.unlock();
            }
        }
    }

    /**
     * 从 DB 查询短链并回填缓存。
     *
     * <p>查询条件：shortCode = ? AND del_flag = 0（ShardingSphere 按 short_code 精确路由）。
     * 查到后校验状态与过期时间，过期则清理缓存。</p>
     *
     * @param shortCode 短码
     * @param redisKey  Redis 缓存 key
     * @return 原始长链
     */
    private String loadFromDbAndBackfill(String shortCode, String redisKey) {
        // 查 DB（ShardingSphere 按 short_code 精确路由）
        ShortLinkDO shortLinkDO = shortLinkMapper.selectOne(
                new LambdaQueryWrapper<ShortLinkDO>()
                        .eq(ShortLinkDO::getShortCode, shortCode)
                        .eq(ShortLinkDO::getDelFlag, 0)
        );

        // 查不到 → 短链不存在
        if (shortLinkDO == null) {
            log.warn("短链不存在：shortCode={}", shortCode);
            throw new BizException(ErrorCode.SHORT_LINK_NOT_FOUND);
        }

        // status == 1 → 短链已被禁用
        if (Integer.valueOf(1).equals(shortLinkDO.getStatus())) {
            log.warn("短链已被禁用：shortCode={}", shortCode);
            throw new BizException(ErrorCode.SHORT_LINK_BANNED);
        }

        // 过期校验：expireTime != null && expireTime.isBefore(now) → 已过期，清理缓存
        if (shortLinkDO.getExpireTime() != null
                && shortLinkDO.getExpireTime().isBefore(LocalDateTime.now())) {
            log.warn("短链已过期：shortCode={}, expireTime={}", shortCode, shortLinkDO.getExpireTime());
            evictCache(shortCode, redisKey);
            throw new BizException(ErrorCode.SHORT_LINK_EXPIRED);
        }

        // ===== 回填 Redis（TTL = CACHE_SHORT_LINK_TTL + 随机抖动 0~300 秒，防雪崩） =====
        String json = JSONUtil.toJsonStr(shortLinkDO);
        long ttl = ShortLinkConstants.CACHE_SHORT_LINK_TTL
                + ThreadLocalRandom.current().nextInt(CACHE_TTL_JITTER_BOUND);
        stringRedisTemplate.opsForValue().set(redisKey, json, ttl, TimeUnit.SECONDS);

        // ===== 回填 Caffeine（暂不限制，后续 Task 11 热点探测控制） =====
        shortLinkLocalCache.put(shortCode, json);

        log.info("DB 查询并回填缓存完成：shortCode={}", shortCode);

        // 异步上报访问记录到本地队列 → Kafka（不影响跳转 RT）
        reportAccessRecord(shortCode);

        return shortLinkDO.getOriginalUrl();
    }

    /**
     * 反序列化短链 JSON 为 ShortLinkDO。
     *
     * @param json      JSON 字符串
     * @param shortCode 短码（仅用于日志）
     * @return ShortLinkDO，反序列化失败返回 null
     */
    private ShortLinkDO parseShortLink(String json, String shortCode) {
        try {
            return JSONUtil.toBean(json, ShortLinkDO.class);
        } catch (Exception e) {
            log.error("反序列化短链缓存失败：shortCode={}, json={}", shortCode, json, e);
            return null;
        }
    }

    /**
     * 清理过期短链的缓存（Redis + Caffeine）。
     *
     * @param shortCode 短码
     * @param redisKey  Redis 缓存 key
     */
    private void evictCache(String shortCode, String redisKey) {
        // 删除 Redis 缓存
        stringRedisTemplate.delete(redisKey);
        // 删除 Caffeine 缓存
        shortLinkLocalCache.invalidate(shortCode);
    }

    /**
     * 安静地休眠（不抛出 InterruptedException，被中断时恢复中断标志位）。
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

    // ==================== 异步访问记录上报（Task 17） ====================

    /**
     * 异步上报访问记录到本地队列 → Kafka。
     *
     * <p>从 {@link RequestContextHolder} 获取当前 HTTP 请求，提取 IP、User-Agent 等信息，
     * 构建访问记录 JSON 并投递到 {@link AccessRecordQueue}（非阻塞，O(1)）。</p>
     *
     * <p>设计要点：</p>
     * <ul>
     *   <li>全程 try-catch 包裹，任何异常仅记录日志，不影响跳转主流程；</li>
     *   <li>队列满时 {@link AccessRecordQueue#offer} 返回 false 并记录日志（丢弃策略）；</li>
     *   <li>UA 解析委托给 {@link UserAgentUtils}，地域暂记 "unknown"（实际可用 IP 库解析）。</li>
     * </ul>
     *
     * @param shortCode 短码
     */
    private void reportAccessRecord(String shortCode) {
        try {
            HttpServletRequest request = getCurrentRequest();
            String ip = request != null ? IpUtils.getIpAddr(request) : IpUtils.UNKNOWN;
            String userAgent = request != null ? request.getHeader("User-Agent") : "";

            JSONObject record = new JSONObject();
            record.set("shortCode", shortCode);
            record.set("accessTime", LocalDateTime.now().toString());
            record.set("userIdentifier", generateUserIdentifier(ip, userAgent));
            record.set("ip", ip);
            record.set("userAgent", userAgent);
            // 解析 UA
            record.set("browser", UserAgentUtils.parseBrowser(userAgent));
            record.set("os", UserAgentUtils.parseOs(userAgent));
            record.set("device", UserAgentUtils.parseDevice(userAgent));
            record.set("network", UserAgentUtils.parseNetwork(userAgent));
            // 地域（简化，实际可用 IP 库解析）
            record.set("locale", "unknown");
            accessRecordQueue.offer(record.toString());
        } catch (Exception e) {
            log.warn("异步上报访问记录失败", e);
        }
    }

    /**
     * 从 Spring 请求上下文获取当前 HttpServletRequest。
     *
     * <p>使用 {@link RequestContextHolder} 获取当前线程绑定的请求对象，
     * 无需修改 jump 方法签名即可在 Service 层访问 HTTP 请求信息。</p>
     *
     * @return 当前 HttpServletRequest，非 HTTP 请求上下文时返回 null
     */
    private HttpServletRequest getCurrentRequest() {
        ServletRequestAttributes attributes =
                (ServletRequestAttributes) RequestContextHolder.getRequestAttributes();
        return attributes != null ? attributes.getRequest() : null;
    }

    /**
     * 生成用户标识（用于 UV 去重）。
     *
     * <p>简化方案：IP + User-Agent 的 MD5 哈希。</p>
     * <p>实际生产可结合 Cookie、账号 ID 等更精确的标识。</p>
     *
     * @param ip        客户端 IP
     * @param userAgent User-Agent 字符串
     * @return 用户标识（32 位 MD5 十六进制串）
     */
    private String generateUserIdentifier(String ip, String userAgent) {
        return SecureUtil.md5(ip + userAgent);
    }

    // ==================== Sentinel 熔断降级 ====================

    /**
     * Sentinel blockHandler：触发熔断/限流时的降级处理。
     *
     * <p>方法签名 = 原方法参数 + 末尾追加 {@link BlockException}，返回类型与原方法一致。</p>
     *
     * <p>降级策略：DB 和 Redis 可能不可用，只读 Caffeine 本地缓存，
     * 命中则返回对应的 originalUrl，未命中则抛出友好业务异常（保证服务可用性 &gt; 数据一致性）。</p>
     *
     * @param shortCode 短码
     * @param ex        Sentinel BlockException（含熔断/限流类型信息）
     * @return 原始长链（仅本地缓存命中时）
     */
    public String jumpBlockHandler(String shortCode, BlockException ex) {
        log.warn("Sentinel 触发熔断/限流降级：shortCode={}, blockType={}, rule={}",
                shortCode, ex.getClass().getSimpleName(),
                ex.getRule() != null ? ex.getRule().getResource() : "null");
        return fallbackFromLocalCache(shortCode);
    }

    /**
     * Sentinel fallback：jump 方法抛出异常时的降级处理。
     *
     * <p>方法签名 = 原方法参数 + 末尾追加 {@link Throwable}，返回类型与原方法一致。</p>
     *
     * <p>降级策略同 blockHandler：只读 Caffeine 本地缓存，未命中则抛出友好业务异常。
     * 注意：fallback 会捕获除 BlockException 之外的业务异常/系统异常。</p>
     *
     * @param shortCode 短码
     * @param ex        被捕获的异常
     * @return 原始长链（仅本地缓存命中时）
     */
    public String jumpFallback(String shortCode, Throwable ex) {
        log.warn("Sentinel 触发异常降级 fallback：shortCode={}, error={}", shortCode, ex.getMessage(), ex);
        return fallbackFromLocalCache(shortCode);
    }

    /**
     * 降级统一策略：只读 Caffeine 本地缓存。
     *
     * <p>熔断场景下 DB 和 Redis 可能不可用，仅尝试读取本地缓存：
     * <ul>
     *   <li>命中且反序列化成功 → 返回 originalUrl；</li>
     *   <li>未命中或反序列化失败 → 抛 {@link BizException}（SHORT_LINK_NOT_FOUND），
     *       返回友好错误信息，避免直接返回 500。</li>
     * </ul>
     *
     * @param shortCode 短码
     * @return 原始长链
     */
    private String fallbackFromLocalCache(String shortCode) {
        String localJson = shortLinkLocalCache.getIfPresent(shortCode);
        if (localJson != null) {
            ShortLinkDO hit = parseShortLink(localJson, shortCode);
            if (hit != null && hit.getOriginalUrl() != null) {
                log.info("降级命中本地缓存：shortCode={}", shortCode);
                return hit.getOriginalUrl();
            }
        }
        log.warn("降级未命中本地缓存，返回友好错误：shortCode={}", shortCode);
        throw new BizException(ErrorCode.SHORT_LINK_NOT_FOUND, "服务降级，短链信息暂不可用");
    }
}
