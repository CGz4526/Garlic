package com.garlic.shortlink;

import com.garlic.shortlink.common.bloom.BloomFilterService;
import com.garlic.shortlink.common.constant.ShortLinkConstants;
import com.garlic.shortlink.dao.entity.ShortLinkDO;
import com.garlic.shortlink.dao.mapper.ShortLinkMapper;
import com.garlic.shortlink.dto.ShortLinkCreateReqDTO;
import com.garlic.shortlink.dto.ShortLinkCreateRespDTO;
import com.garlic.shortlink.service.shortcode.ShortCodeGenerator;
import com.garlic.shortlink.service.shortlink.ShortLinkCreateService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * ShortLinkCreateService 单元测试（纯 Mockito，不依赖 Spring 容器与真实 Redis/DB）。
 *
 * <p>验证幂等逻辑：当 Redis 已存在 longUrl 的幂等映射时，应直接返回对应 shortCode，
 * 不调用 shortLinkMapper.insert()。</p>
 *
 * @author garlic
 */
@ExtendWith(MockitoExtension.class)
class ShortLinkCreateServiceTest {

    @Mock
    private ShortLinkMapper shortLinkMapper;

    @Mock
    private StringRedisTemplate stringRedisTemplate;

    @Mock
    private ShortCodeGenerator shortCodeGenerator;

    @Mock
    private RedissonClient redissonClient;

    @Mock
    private BloomFilterService bloomFilterService;

    @InjectMocks
    private ShortLinkCreateService shortLinkCreateService;

    @Mock
    private ValueOperations<String, String> valueOperations;

    @BeforeEach
    void setUp() {
        // mock stringRedisTemplate.opsForValue() 返回 mock 的 ValueOperations
        lenient().when(stringRedisTemplate.opsForValue()).thenReturn(valueOperations);
    }

    /**
     * 计算与 Service 内部一致的幂等 hashKey。
     */
    private String buildHashKey(Long userId, String originalUrl) {
        return ShortLinkConstants.IDEMPOTENT_KEY_PREFIX + userId + ":" + Math.abs(originalUrl.hashCode());
    }

    /**
     * 幂等命中场景：Redis 已存在 longUrl 的幂等映射，应直接返回 shortCode，
     * 不调用 DB insert、不调用短码生成器、不获取分布式锁。
     */
    @Test
    void createShortLink_idempotentHit_shouldReturnExistingShortCode() {
        // 准备请求
        ShortLinkCreateReqDTO req = new ShortLinkCreateReqDTO();
        req.setOriginalUrl("https://www.example.com/page");
        req.setUserId(1L);

        String originalUrl = req.getOriginalUrl();
        String hashKey = buildHashKey(req.getUserId(), originalUrl);

        // mock 幂等映射命中（opsForValue().get(hashKey) 返回已存在的 shortCode）
        String existingShortCode = "abc123";
        when(valueOperations.get(hashKey)).thenReturn(existingShortCode);

        // 执行
        ShortLinkCreateRespDTO resp = shortLinkCreateService.createShortLink(req);

        // 验证：返回的 shortCode 与已存在映射一致
        assertNotNull(resp);
        assertEquals(existingShortCode, resp.getShortCode());
        assertEquals(originalUrl, resp.getOriginalUrl());
        assertEquals(ShortLinkConstants.DEFAULT_DOMAIN + existingShortCode, resp.getShortUrl());

        // 验证：幂等命中时不应调用 DB insert
        verify(shortLinkMapper, never()).insert(any(ShortLinkDO.class));
        // 验证：不应调用短码生成器
        verify(shortCodeGenerator, never()).generateShortCode();
        verify(shortCodeGenerator, never()).generateShortCodeWithConflictCheck(any());
        // 验证：不应获取分布式锁
        verify(redissonClient, never()).getLock(anyString());
    }

    /**
     * 幂等未命中场景：Redis 无映射，应走完整创建流程
     * （生成短码 + 写 DB + 写缓存 + 写幂等映射 + 释放锁）。
     */
    @Test
    void createShortLink_idempotentMiss_shouldCreateNewShortLink() throws InterruptedException {
        // 准备请求
        ShortLinkCreateReqDTO req = new ShortLinkCreateReqDTO();
        req.setOriginalUrl("https://www.example.com/new-page");
        req.setUserId(1L);

        // mock 幂等未命中（所有 get 均返回 null）
        when(valueOperations.get(anyString())).thenReturn(null);

        // mock 分布式锁：拿到锁
        RLock mockLock = mock(RLock.class);
        when(redissonClient.getLock(anyString())).thenReturn(mockLock);
        when(mockLock.tryLock(0, 10, TimeUnit.SECONDS)).thenReturn(true);
        when(mockLock.isHeldByCurrentThread()).thenReturn(true);

        // mock 短码生成
        String newShortCode = "xyz789";
        when(shortCodeGenerator.generateShortCodeWithConflictCheck(any())).thenReturn(newShortCode);

        // mock DB insert
        when(shortLinkMapper.insert(any(ShortLinkDO.class))).thenReturn(1);

        // 执行
        ShortLinkCreateRespDTO resp = shortLinkCreateService.createShortLink(req);

        // 验证：返回新短码
        assertNotNull(resp);
        assertEquals(newShortCode, resp.getShortCode());
        assertEquals(ShortLinkConstants.DEFAULT_DOMAIN + newShortCode, resp.getShortUrl());
        assertEquals(req.getOriginalUrl(), resp.getOriginalUrl());

        // 验证：调用了 DB insert 1 次
        verify(shortLinkMapper, times(1)).insert(any(ShortLinkDO.class));
        // 验证：调用了短码生成器 1 次
        verify(shortCodeGenerator, times(1)).generateShortCodeWithConflictCheck(any());
        // 验证：写入了 2 次 set（缓存 + 幂等映射）
        verify(valueOperations, times(2)).set(anyString(), anyString(), anyLong(), any(TimeUnit.class));
        // 验证：释放了锁
        verify(mockLock, times(1)).unlock();
    }
}
