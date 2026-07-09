package com.garlic.shortlink.service.cache;

import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.github.benmanes.caffeine.cache.Cache;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Kafka 缓存失效广播消费者。
 *
 * <p>监听 {@link com.garlic.shortlink.common.constant.ShortLinkConstants#TOPIC_CACHE_INVALID}
 * 主题，收到其他实例发送的缓存失效消息后，清理本地 Caffeine 缓存，保证多实例间
 * L1 缓存一致性。</p>
 *
 * <p><b>广播消费说明</b>：</p>
 * <ul>
 *     <li>Kafka 消费者组（groupId）决定消费模式：相同 groupId 下同一分区只被一个消费者消费，
 *         不同 groupId 的消费者都会收到同一条消息（即广播）；</li>
 *     <li>本消费者使用固定 groupId = {@code short-link-cache-invalid-group}，
 *         <b>实际生产中每个实例应使用不同 groupId</b>（如以 instanceId 为后缀）才能实现
 *         真正的广播消费，让所有实例都收到消息并清理各自本地缓存；</li>
 *     <li>当前实现为简化版本，单实例运行无影响；多实例部署需改造 groupId 生成策略
 *         （例如通过 SpEL 引用 {@code cacheConsistencyService.instanceId} 或
 *         在 application.yml 中按实例注入不同 group-id）。</li>
 * </ul>
 *
 * <p>消息来源去重：通过消息体中的 {@code instanceId} 字段与当前实例 ID 比较，
 * 跳过自己发送的消息，避免重复处理。</p>
 *
 * @author garlic
 */
@Slf4j
@Component
public class CacheInvalidListener {

    /** 注入 Caffeine 本地缓存（L1，Bean 名 shortLinkLocalCache） */
    private final Cache<String, String> caffeineCache;

    /** 注入缓存一致性服务（用于获取 instanceId 判断消息来源） */
    private final CacheConsistencyService cacheConsistencyService;

    /**
     * 构造方法：注入所有依赖。
     *
     * @param caffeineCache           Caffeine 本地缓存（Bean 名 shortLinkLocalCache）
     * @param cacheConsistencyService 缓存一致性服务（提供 instanceId）
     */
    public CacheInvalidListener(
            @Qualifier("shortLinkLocalCache") Cache<String, String> caffeineCache,
            CacheConsistencyService cacheConsistencyService) {
        this.caffeineCache = caffeineCache;
        this.cacheConsistencyService = cacheConsistencyService;
    }

    /**
     * 消费缓存失效广播消息。
     *
     * <p>流程：</p>
     * <ol>
     *     <li>解析 JSON 消息获取 shortCode 和 sourceInstanceId；</li>
     *     <li>若 sourceInstanceId == 当前 instanceId，跳过（不处理自己发的消息）；</li>
     *     <li>清理本地 Caffeine 缓存。</li>
     * </ol>
     *
     * <p>注：使用固定 groupId {@code short-link-cache-invalid-group}。
     * 实际生产中每个实例应使用不同 groupId 实现广播消费（不同 groupId 的消费者
     * 都会收到同一条消息）。当前简化版本仅用于演示，多实例部署时需改造 groupId。</p>
     *
     * @param message Kafka 消息体（JSON 字符串）
     */
    @KafkaListener(topics = "short-link-cache-invalid", groupId = "short-link-cache-invalid-group")
    public void onCacheInvalid(String message) {
        try {
            JSONObject json = JSONUtil.parseObj(message);
            String shortCode = json.getStr("shortCode");
            String sourceInstanceId = json.getStr("instanceId");

            // 1. 若消息来源是当前实例，跳过（不处理自己发的消息）
            if (cacheConsistencyService.getInstanceId().equals(sourceInstanceId)) {
                log.debug("跳过自己发送的缓存失效消息：shortCode={}, sourceInstanceId={}",
                        shortCode, sourceInstanceId);
                return;
            }

            // 2. 清理本地 Caffeine 缓存
            caffeineCache.invalidate(shortCode);
            log.info("收到缓存失效广播，清理本地 Caffeine：shortCode={}, sourceInstanceId={}, currentInstanceId={}",
                    shortCode, sourceInstanceId, cacheConsistencyService.getInstanceId());
        } catch (Exception e) {
            log.error("处理缓存失效广播消息失败：message={}", message, e);
        }
    }
}
