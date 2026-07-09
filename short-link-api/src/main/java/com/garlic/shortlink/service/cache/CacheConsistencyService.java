package com.garlic.shortlink.service.cache;

import cn.hutool.json.JSONUtil;
import com.garlic.shortlink.common.constant.ShortLinkConstants;
import com.github.benmanes.caffeine.cache.Cache;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * 缓存一致性服务（延迟双删 + Kafka 广播）。
 *
 * <p>简历亮点 1 的缓存一致性核心实现，针对「先更新 DB，后删缓存」场景下
 * 主从复制延迟 / 并发读写导致的脏缓存问题。</p>
 *
 * <p>方案流程（延迟双删）：</p>
 * <ol>
 *     <li><b>第一次删 Redis</b>：更新 DB 前先删除 Redis 缓存，避免旧值被并发读到；</li>
 *     <li><b>执行 DB 更新</b>：调用方通过 {@code dbUpdate} 传入 DB 写入逻辑；</li>
 *     <li><b>延迟二次删除</b>（异步，500ms 后执行）：
 *         <ul>
 *             <li>再次删除 Redis（清除主从复制延迟窗口内被旧值回填的缓存）；</li>
 *             <li>清理本地 Caffeine（L1）；</li>
 *             <li>发送 Kafka 广播消息，通知其他实例清理各自的 Caffeine。</li>
 *         </ul>
 *     </li>
 * </ol>
 *
 * <p>Kafka 广播：使用 shortCode 作为消息 key，保证同一 shortCode 的消息落到同一分区，
 * 顺序消费，避免乱序导致的脏缓存问题。</p>
 *
 * @author garlic
 */
@Slf4j
@Service
public class CacheConsistencyService {

    /** 注入 StringRedisTemplate（L2 Redis 缓存清理） */
    private final StringRedisTemplate stringRedisTemplate;

    /** 注入 KafkaTemplate（缓存失效广播） */
    private final KafkaTemplate<String, String> kafkaTemplate;

    /** 注入 Caffeine 本地缓存（L1，Bean 名 shortLinkLocalCache） */
    private final Cache<String, String> caffeineCache;

    /** 当前实例 ID（用于广播消息去重，构造时生成 UUID） */
    private final String instanceId;

    /** 延迟双删调度器（守护线程，核心 2 线程） */
    private ScheduledExecutorService scheduledExecutor;

    /** 延迟二次删除的延迟时间（毫秒） */
    private static final long SECOND_DELETE_DELAY_MS = 500L;

    /**
     * 构造方法：注入所有依赖并生成当前实例 ID。
     *
     * @param stringRedisTemplate StringRedisTemplate（L2）
     * @param kafkaTemplate        KafkaTemplate（广播）
     * @param caffeineCache        Caffeine 本地缓存（L1，Bean 名 shortLinkLocalCache）
     */
    public CacheConsistencyService(
            StringRedisTemplate stringRedisTemplate,
            KafkaTemplate<String, String> kafkaTemplate,
            @Qualifier("shortLinkLocalCache") Cache<String, String> caffeineCache) {
        this.stringRedisTemplate = stringRedisTemplate;
        this.kafkaTemplate = kafkaTemplate;
        this.caffeineCache = caffeineCache;
        this.instanceId = UUID.randomUUID().toString();
    }

    /**
     * 初始化延迟双删调度器：核心 2 线程的守护线程池。
     *
     * <p>守护线程保证 JVM 退出时不会阻塞，@PreDestroy 时显式关闭。</p>
     */
    @PostConstruct
    public void init() {
        scheduledExecutor = Executors.newScheduledThreadPool(2, r -> {
            Thread t = new Thread(r, "cache-double-delete-scheduler");
            t.setDaemon(true);
            return t;
        });
        log.info("CacheConsistencyService 初始化完成：instanceId={}", instanceId);
    }

    /**
     * 销毁时关闭调度器，避免线程泄漏。
     */
    @PreDestroy
    public void destroy() {
        if (scheduledExecutor != null) {
            scheduledExecutor.shutdown();
            log.info("CacheConsistencyService 调度器已关闭：instanceId={}", instanceId);
        }
    }

    /**
     * 获取当前实例 ID（供消费者判断是否为自己发的消息）。
     *
     * @return 实例 ID
     */
    public String getInstanceId() {
        return instanceId;
    }

    /**
     * 延迟双删 + Kafka 广播清理缓存。
     *
     * <p>流程：</p>
     * <ol>
     *     <li>第一次删 Redis（更新 DB 前先清缓存，避免并发读到旧值）；</li>
     *     <li>执行 DB 更新（由调用方传入 Runnable）；</li>
     *     <li>异步延迟 500ms 后二次删除：删 Redis + 清本地 Caffeine + Kafka 广播。</li>
     * </ol>
     *
     * <p>使用场景：先删缓存后更新 DB 的强一致场景。若调用方已先完成 DB 更新，
     * 可传入空 {@code dbUpdate}（如 {@code () -> {}}），仅享受延迟双删 + 广播能力。</p>
     *
     * @param shortCode 短码
     * @param dbUpdate  DB 更新逻辑（可传空 Runnable）
     */
    public void evictWithDoubleDelete(String shortCode, Runnable dbUpdate) {
        String cacheKey = ShortLinkConstants.CACHE_SHORT_LINK_KEY_PREFIX + shortCode;

        // 1. 第一次删 Redis（更新 DB 前先清缓存，避免并发读到旧值）
        Boolean deleted = stringRedisTemplate.delete(cacheKey);
        log.info("延迟双删-第一次删除 Redis：shortCode={}, deleted={}", shortCode, deleted);

        // 2. 执行 DB 更新
        try {
            dbUpdate.run();
        } catch (Exception e) {
            log.error("延迟双删-DB 更新失败：shortCode={}", shortCode, e);
            throw e;
        }

        // 3. 延迟二次删除（异步 500ms 后执行：再次删 Redis + 清本地 Caffeine + Kafka 广播）
        scheduledExecutor.schedule(() -> {
            try {
                // 再次删 Redis（清除主从复制延迟窗口内被旧值回填的缓存）
                Boolean secondDeleted = stringRedisTemplate.delete(cacheKey);
                log.info("延迟双删-第二次删除 Redis：shortCode={}, deleted={}", shortCode, secondDeleted);

                // 同时清理本地 Caffeine
                caffeineCache.invalidate(shortCode);
                log.info("延迟双删-清理本地 Caffeine：shortCode={}", shortCode);

                // 发送 Kafka 广播清理其他实例的 Caffeine
                sendCacheInvalidMessage(shortCode);
            } catch (Exception e) {
                log.error("延迟双删-二次删除失败：shortCode={}", shortCode, e);
            }
        }, SECOND_DELETE_DELAY_MS, TimeUnit.MILLISECONDS);
    }

    /**
     * 发送缓存失效广播消息到 Kafka。
     *
     * <p>消息体 JSON 格式：</p>
     * <pre>{@code
     * {"shortCode": "xxx", "timestamp": 1234567890, "instanceId": "xxx"}
     * }</pre>
     *
     * <p>使用 shortCode 作为消息 key，Kafka 会按 key 分区，保证同一 shortCode 的消息
     * 落到同一分区，顺序消费，避免乱序。</p>
     *
     * @param shortCode 短码
     */
    public void sendCacheInvalidMessage(String shortCode) {
        Map<String, Object> message = new HashMap<>(3);
        message.put("shortCode", shortCode);
        message.put("timestamp", System.currentTimeMillis());
        message.put("instanceId", instanceId);
        String jsonMessage = JSONUtil.toJsonStr(message);

        // shortCode 作为 key，Kafka 按 key 分区，同 shortCode 在同一分区
        kafkaTemplate.send(ShortLinkConstants.TOPIC_CACHE_INVALID, shortCode, jsonMessage);
        log.info("发送缓存失效广播消息：topic={}, shortCode={}, instanceId={}",
                ShortLinkConstants.TOPIC_CACHE_INVALID, shortCode, instanceId);
    }
}
