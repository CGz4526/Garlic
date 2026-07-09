package com.garlic.shortlink.concurrent;

import com.garlic.shortlink.common.bloom.BloomFilterService;
import com.garlic.shortlink.common.constant.ShortLinkConstants;
import com.garlic.shortlink.common.exception.BizException;
import com.garlic.shortlink.common.exception.ErrorCode;
import com.garlic.shortlink.dao.entity.ShortLinkDO;
import com.garlic.shortlink.dao.mapper.ShortLinkMapper;
import com.garlic.shortlink.dto.ShortLinkCreateReqDTO;
import com.garlic.shortlink.dto.ShortLinkCreateRespDTO;
import com.garlic.shortlink.service.shortcode.ShortCodeGenerator;
import com.garlic.shortlink.service.shortlink.ShortLinkCreateService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.startsWith;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * 并发幂等测试：验证 {@link ShortLinkCreateService#createShortLink} 在高并发场景下的幂等性。
 *
 * <p>测试策略：</p>
 * <ul>
 *   <li>使用 {@link CountDownLatch} 栅栏保证 20 个线程同时发起相同长链的创建请求；</li>
 *   <li>使用 {@link CompletableFuture#allOf} 等待所有线程完成；</li>
 *   <li>使用 {@link AtomicInteger#compareAndSet} 模拟 Redisson 分布式锁（SETNX），
 *       保证只有一个线程能获取到锁，其余线程抛 {@link ErrorCode#IDEMPOTENT_REJECT}；</li>
 *   <li>验证 DB insert 仅被调用 1 次，Redis 幂等映射 SET 仅成功 1 次。</li>
 * </ul>
 *
 * <p>说明：Mockito 的 {@code when().thenReturn()} 在并发下返回常量值是线程安全的，
 * 但需要控制「锁竞争只有一个赢家」时，必须使用 {@code doAnswer} + {@code AtomicInteger}
 * 保证 CAS 语义。</p>
 *
 * @author garlic
 */
@ExtendWith(MockitoExtension.class)
class IdempotencyConcurrencyTest {

    private static final int THREAD_COUNT = 20;
    private static final String CONCURRENT_URL = "https://www.example.com/concurrent-idempotent";

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

    @Mock
    private ValueOperations<String, String> valueOperations;

    /** 被测对象 */
    private ShortLinkCreateService shortLinkCreateService;

    @BeforeEach
    void setUp() {
        // 使用 lenient 避免严格模式下的 UnnecessaryStubbingException
        lenient().when(stringRedisTemplate.opsForValue()).thenReturn(valueOperations);
        // 手动构造被测对象，确保所有依赖注入（@InjectMocks 在并发测试中可能因字段顺序导致注入异常）
        shortLinkCreateService = new ShortLinkCreateService(
                shortLinkMapper, stringRedisTemplate, shortCodeGenerator,
                redissonClient, bloomFilterService);
    }

    /**
     * 并发幂等测试：20 个线程同时调用 createShortLink 传入相同长链，
     * 由于分布式锁保护，最终只生成 1 个短码（DB insert 仅 1 次，幂等映射 SET 仅 1 次）。
     *
     * <p>预期结果：</p>
     * <ul>
     *   <li>1 个线程成功获取锁，完成 DB insert + 缓存写入 + 幂等映射写入，返回新短码；</li>
     *   <li>19 个线程获取锁失败，抛 {@link BizException}（IDEMPOTENT_REJECT）；</li>
     *   <li>DB insert 调用次数 = 1；</li>
     *   <li>幂等映射 SET 调用次数 = 1（模拟 Redis SETNX 只成功一次）。</li>
     * </ul>
     */
    @Test
    @DisplayName("并发幂等测试：20 线程同时创建相同长链，DB insert 与 SETNX 均仅 1 次")
    void createShortLink_concurrentSameUrl_shouldOnlyInsertOnce() throws Exception {
        // ====== 并发控制变量 ======
        // CAS 控制锁竞争：初始值 0，只有第一个调用 compareAndSet(0, 1) 成功的线程获取锁
        AtomicInteger lockAcquiredCount = new AtomicInteger(0);
        // DB insert 调用计数（仅获取到锁的线程会到达此步）
        AtomicInteger dbInsertCount = new AtomicInteger(0);

        // ====== Mock 分布式锁（模拟 Redisson SETNX 行为） ======
        RLock mockLock = mock(RLock.class);
        when(redissonClient.getLock(anyString())).thenReturn(mockLock);

        // 使用 doAnswer + compareAndSet 保证锁只被一个线程获取（线程安全）
        doAnswer(invocation -> {
            // CAS：只有第一个线程能从 0→1，返回 true；其余返回 false
            return lockAcquiredCount.compareAndSet(0, 1);
        }).when(mockLock).tryLock(anyLong(), anyLong(), any(TimeUnit.class));

        // 仅获取锁的线程在 finally 中调用 isHeldByCurrentThread + unlock
        lenient().when(mockLock.isHeldByCurrentThread()).thenReturn(true);

        // ====== Mock 幂等检查（所有线程第一次查 Redis 均未命中） ======
        lenient().when(valueOperations.get(anyString())).thenReturn(null);

        // ====== Mock DB insert（计数，仅获取锁的线程会调用） ======
        doAnswer(invocation -> {
            dbInsertCount.incrementAndGet();
            return 1;
        }).when(shortLinkMapper).insert(any(ShortLinkDO.class));

        // ====== Mock 短码生成器 ======
        when(shortCodeGenerator.generateShortCodeWithConflictCheck(any())).thenReturn("concurrent001");

        // ====== Mock 布隆过滤器添加 ======
        lenient().when(bloomFilterService.add(anyString())).thenReturn(true);

        // ====== 构造 20 个并发任务 ======
        ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);
        CountDownLatch startLatch = new CountDownLatch(1);

        List<CompletableFuture<Void>> futures = new ArrayList<>(THREAD_COUNT);
        List<ShortLinkCreateRespDTO> successResults = Collections.synchronizedList(new ArrayList<>());
        List<Throwable> failures = Collections.synchronizedList(new ArrayList<>());

        for (int i = 0; i < THREAD_COUNT; i++) {
            CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                try {
                    // 等待发令枪，保证所有线程同时开始
                    startLatch.await();
                    ShortLinkCreateReqDTO req = new ShortLinkCreateReqDTO();
                    req.setOriginalUrl(CONCURRENT_URL);
                    req.setUserId(1L);
                    ShortLinkCreateRespDTO resp = shortLinkCreateService.createShortLink(req);
                    successResults.add(resp);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    failures.add(e);
                } catch (Throwable e) {
                    failures.add(e);
                }
            }, executor);
            futures.add(future);
        }

        // ====== 发令枪：所有线程同时开始 ======
        startLatch.countDown();

        // ====== 使用 CompletableFuture.allOf 等待所有线程完成 ======
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                .get(30, TimeUnit.SECONDS);
        executor.shutdown();

        // ====== 验证结果 ======

        // 1. 成功的线程数 = 1（获取到锁的线程）
        assertThat(successResults)
                .as("只有 1 个线程应成功创建短链")
                .hasSize(1);
        assertThat(successResults.get(0).getShortCode()).isEqualTo("concurrent001");

        // 2. 失败的线程数 = 19（未获取到锁，抛 IDEMPOTENT_REJECT）
        assertThat(failures)
                .as("19 个线程应因锁竞争失败抛 IDEMPOTENT_REJECT")
                .hasSize(THREAD_COUNT - 1);

        // 3. 所有失败均为 BizException(IDEMPOTENT_REJECT)
        assertThat(failures).allSatisfy(t -> {
            assertThat(t).isInstanceOf(BizException.class);
            assertThat(((BizException) t).getCode())
                    .isEqualTo(ErrorCode.IDEMPOTENT_REJECT.getCode());
        });

        // 4. DB insert 仅被调用 1 次（核心幂等验证）
        assertThat(dbInsertCount.get())
                .as("DB insert 应仅被调用 1 次")
                .isEqualTo(1);

        // 5. 分布式锁（SETNX）仅成功 1 次
        assertThat(lockAcquiredCount.get())
                .as("分布式锁 SETNX 应仅成功 1 次")
                .isEqualTo(1);

        // 6. 验证幂等映射 SET 仅被调用 1 次（Redis SETNX 语义）
        verify(valueOperations, times(1)).set(
                startsWith(ShortLinkConstants.IDEMPOTENT_KEY_PREFIX),
                anyString(), anyLong(), any(TimeUnit.class));

        // 7. 验证 DB insert 被 Mockito verify 确认仅 1 次
        verify(shortLinkMapper, times(1)).insert(any(ShortLinkDO.class));
    }
}
