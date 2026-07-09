package com.garlic.shortlink;

import cn.hutool.json.JSONUtil;
import com.garlic.shortlink.common.bloom.BloomFilterService;
import com.garlic.shortlink.common.exception.BizException;
import com.garlic.shortlink.common.exception.ErrorCode;
import com.garlic.shortlink.dao.entity.ShortLinkDO;
import com.garlic.shortlink.dao.mapper.ShortLinkMapper;
import com.garlic.shortlink.service.mq.AccessRecordQueue;
import com.garlic.shortlink.service.shortlink.ShortLinkJumpService;
import com.github.benmanes.caffeine.cache.Cache;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;

import java.time.LocalDateTime;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * ShortLinkJumpService 单元测试（纯 Mockito，不依赖真实 Redis/Caffeine/DB）。
 *
 * <p>覆盖三级缓存链路：L1 Caffeine → L2 Redis → DB，以及布隆过滤器拦截、过期短链等场景。</p>
 *
 * @author garlic
 */
@ExtendWith(MockitoExtension.class)
class ShortLinkJumpServiceTest {

    @Mock
    private StringRedisTemplate stringRedisTemplate;

    @Mock
    private BloomFilterService bloomFilterService;

    @Mock
    private ShortLinkMapper shortLinkMapper;

    @Mock
    private Cache<String, String> shortLinkLocalCache;

    @Mock
    private RedissonClient redissonClient;

    @Mock
    private AccessRecordQueue accessRecordQueue;

    @Mock
    private ValueOperations<String, String> valueOperations;

    @InjectMocks
    private ShortLinkJumpService shortLinkJumpService;

    @BeforeEach
    void setUp() {
        lenient().when(stringRedisTemplate.opsForValue()).thenReturn(valueOperations);
    }

    /**
     * 构造一个正常状态的 ShortLinkDO。
     */
    private ShortLinkDO buildNormalShortLink(String shortCode, String originalUrl) {
        ShortLinkDO shortLinkDO = new ShortLinkDO();
        shortLinkDO.setShortCode(shortCode);
        shortLinkDO.setOriginalUrl(originalUrl);
        shortLinkDO.setStatus(0);
        shortLinkDO.setDelFlag(0);
        shortLinkDO.setExpireTime(null);
        return shortLinkDO;
    }

    /**
     * 缓存命中场景：L1 Caffeine 命中，直接返回，不应查 Redis/DB。
     */
    @Test
    @DisplayName("缓存命中：L1 Caffeine 命中，不查 Redis/DB")
    void jump_caffeineHit_shouldReturnDirectlyWithoutRedisOrDb() {
        String shortCode = "abc123";
        String originalUrl = "https://www.example.com/page";
        ShortLinkDO cached = buildNormalShortLink(shortCode, originalUrl);
        String json = JSONUtil.toJsonStr(cached);

        when(bloomFilterService.contains(shortCode)).thenReturn(true);
        when(shortLinkLocalCache.getIfPresent(shortCode)).thenReturn(json);

        String result = shortLinkJumpService.jump(shortCode);

        assertThat(result).isEqualTo(originalUrl);
        verify(stringRedisTemplate, never()).opsForValue();
        verify(shortLinkMapper, never()).selectOne(any());
    }

    /**
     * 缓存未命中场景：L1 Caffeine 未命中，L2 Redis 命中，回填 Caffeine 后返回。
     */
    @Test
    @DisplayName("缓存未命中走 Redis：L2 Redis 命中并回填 Caffeine")
    void jump_caffeineMissRedisHit_shouldBackfillCaffeine() {
        String shortCode = "xyz789";
        String originalUrl = "https://www.example.com/redis";
        ShortLinkDO cached = buildNormalShortLink(shortCode, originalUrl);
        String json = JSONUtil.toJsonStr(cached);

        when(bloomFilterService.contains(shortCode)).thenReturn(true);
        when(shortLinkLocalCache.getIfPresent(shortCode)).thenReturn(null);
        when(valueOperations.get("short:link:" + shortCode)).thenReturn(json);

        String result = shortLinkJumpService.jump(shortCode);

        assertThat(result).isEqualTo(originalUrl);
        // 验证回填 Caffeine
        verify(shortLinkLocalCache, times(1)).put(eq(shortCode), eq(json));
        // 验证未查 DB
        verify(shortLinkMapper, never()).selectOne(any());
        // 验证未获取分布式锁
        verify(redissonClient, never()).getLock(anyString());
    }

    /**
     * Redis 未命中走 DB：L1/L2 均未命中，加锁后双重检查 Redis 仍未命中，查 DB 并回填。
     */
    @Test
    @DisplayName("Redis 未命中走 DB：加锁双重检查后查 DB 并回填缓存")
    void jump_redisMissDbHit_shouldQueryDbAndBackfill() throws Exception {
        String shortCode = "db001";
        String originalUrl = "https://www.example.com/db";
        ShortLinkDO shortLinkDO = buildNormalShortLink(shortCode, originalUrl);
        String json = JSONUtil.toJsonStr(shortLinkDO);

        when(bloomFilterService.contains(shortCode)).thenReturn(true);
        when(shortLinkLocalCache.getIfPresent(shortCode)).thenReturn(null);
        // 第一次 Redis 查询 + 加锁后双重检查 Redis 均返回 null
        when(valueOperations.get("short:link:" + shortCode)).thenReturn(null);

        RLock mockLock = mock(RLock.class);
        when(redissonClient.getLock(anyString())).thenReturn(mockLock);
        when(mockLock.tryLock(0, 5, TimeUnit.SECONDS)).thenReturn(true);
        when(mockLock.isHeldByCurrentThread()).thenReturn(true);

        when(shortLinkMapper.selectOne(any())).thenReturn(shortLinkDO);

        String result = shortLinkJumpService.jump(shortCode);

        assertThat(result).isEqualTo(originalUrl);
        // 验证查 DB 一次
        verify(shortLinkMapper, times(1)).selectOne(any());
        // 验证回填 Redis
        verify(valueOperations, times(1)).set(
                eq("short:link:" + shortCode), eq(json), anyLong(), eq(TimeUnit.SECONDS));
        // 验证回填 Caffeine
        verify(shortLinkLocalCache, times(1)).put(eq(shortCode), eq(json));
        // 验证释放锁
        verify(mockLock, times(1)).unlock();
    }

    /**
     * 短码不存在场景：布隆过滤器拦截，直接抛 SHORT_LINK_NOT_FOUND。
     */
    @Test
    @DisplayName("短码不存在：布隆过滤器拦截并抛 SHORT_LINK_NOT_FOUND")
    void jump_bloomFilterReject_shouldThrowNotFoundException() {
        String shortCode = "notexist";

        when(bloomFilterService.contains(shortCode)).thenReturn(false);

        BizException ex = assertThrows(BizException.class,
                () -> shortLinkJumpService.jump(shortCode));

        assertThat(ex.getCode()).isEqualTo(ErrorCode.SHORT_LINK_NOT_FOUND.getCode());
        // 验证未查缓存与 DB
        verify(shortLinkLocalCache, never()).getIfPresent(anyString());
        verify(shortLinkMapper, never()).selectOne(any());
        verify(redissonClient, never()).getLock(anyString());
    }

    /**
     * 过期短链场景：DB 查到但已过期，清理缓存并抛 SHORT_LINK_EXPIRED（Controller 层映射为 410）。
     */
    @Test
    @DisplayName("过期短链返回 410：DB 查到但已过期，清理缓存并抛 SHORT_LINK_EXPIRED")
    void jump_expiredShortLink_shouldThrowExpiredAndEvictCache() throws Exception {
        String shortCode = "exp001";
        ShortLinkDO shortLinkDO = buildNormalShortLink(shortCode, "https://www.example.com/expired");
        // 过期时间设置为过去
        shortLinkDO.setExpireTime(LocalDateTime.now().minusDays(1));

        when(bloomFilterService.contains(shortCode)).thenReturn(true);
        when(shortLinkLocalCache.getIfPresent(shortCode)).thenReturn(null);
        when(valueOperations.get("short:link:" + shortCode)).thenReturn(null);

        RLock mockLock = mock(RLock.class);
        when(redissonClient.getLock(anyString())).thenReturn(mockLock);
        when(mockLock.tryLock(0, 5, TimeUnit.SECONDS)).thenReturn(true);
        when(mockLock.isHeldByCurrentThread()).thenReturn(true);

        when(shortLinkMapper.selectOne(any())).thenReturn(shortLinkDO);

        BizException ex = assertThrows(BizException.class,
                () -> shortLinkJumpService.jump(shortCode));

        assertThat(ex.getCode()).isEqualTo(ErrorCode.SHORT_LINK_EXPIRED.getCode());
        // 验证清理了 Redis 缓存
        verify(stringRedisTemplate, times(1)).delete("short:link:" + shortCode);
        // 验证清理了 Caffeine 缓存
        verify(shortLinkLocalCache, times(1)).invalidate(shortCode);
        // 验证释放锁
        verify(mockLock, times(1)).unlock();
        // 验证未回填缓存
        verify(valueOperations, never()).set(anyString(), anyString(), anyLong(), any(TimeUnit.class));
    }
}
