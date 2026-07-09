package com.garlic.shortlink.common.bloom;

import com.garlic.shortlink.common.constant.ShortLinkConstants;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RBloomFilter;
import org.redisson.api.RedissonClient;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.Collection;

/**
 * 布隆过滤器服务（防缓存穿透核心组件）。
 *
 * <p>基于 Redisson {@link RBloomFilter} 实现分布式布隆过滤器，用于在查询短链前
 * 快速判断 shortCode 是否存在，避免不存在的 key 直接穿透到 DB。</p>
 *
 * <p>特性：</p>
 * <ul>
 *     <li>预期插入量 1000 万，误判率 0.1%；</li>
 *     <li>{@code tryInit} 幂等初始化：已初始化则返回 false 且不重置，保证多实例共用时不丢数据；</li>
 *     <li>支持单条与批量添加、存在性判断、近似计数。</li>
 * </ul>
 *
 * <p><b>注意</b>：布隆过滤器不支持删除元素，删除场景在 Task 16 用双布隆方案处理
 * （参见 {@link ScalableBloomFilter}）。</p>
 *
 * @author garlic
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class BloomFilterService {

    /** 注入 Redisson 客户端 */
    private final RedissonClient redissonClient;

    /** 主布隆过滤器（在 {@link #init()} 中初始化） */
    private RBloomFilter<String> bloomFilter;

    /** 预期插入量：1000 万 */
    private long expectedInsertions = 10000000L;

    /** 误判率：0.1% */
    private double falseProbability = 0.001;

    /**
     * 初始化布隆过滤器。
     *
     * <p>{@code tryInit} 会判断是否已初始化：若已初始化则返回 false 且不重置，
     * 保证应用重启或多个实例共用同一个布隆过滤器时不丢数据。</p>
     */
    @PostConstruct
    public void init() {
        bloomFilter = redissonClient.getBloomFilter(ShortLinkConstants.BLOOM_FILTER_NAME);
        boolean initialized = bloomFilter.tryInit(expectedInsertions, falseProbability);
        if (initialized) {
            log.info("布隆过滤器初始化完成：name={}, expectedInsertions={}, falseProbability={}",
                    ShortLinkConstants.BLOOM_FILTER_NAME, expectedInsertions, falseProbability);
        } else {
            log.info("布隆过滤器已存在，跳过初始化：name={}", ShortLinkConstants.BLOOM_FILTER_NAME);
        }
    }

    /**
     * 添加单个元素到布隆过滤器。
     *
     * <p>用于短链生成时实时写入 shortCode。</p>
     *
     * @param key 元素 key（如 shortCode）
     * @return true 表示元素之前不存在并成功添加
     */
    public boolean add(String key) {
        if (key == null || key.isEmpty()) {
            return false;
        }
        return bloomFilter.add(key);
    }

    /**
     * 批量添加元素到布隆过滤器。
     *
     * <p>用于启动预热或批量生成短链时批量写入，减少多次单条写入的 RTT 开销。
     * 注意 Redisson RBloomFilter 未提供原生批量 API，此处通过循环调用
     * {@link RBloomFilter#add} 实现（Pipeline 由 Redisson 内部优化）。</p>
     *
     * @param keys 元素 key 集合
     */
    public void addBatch(Collection<String> keys) {
        if (keys == null || keys.isEmpty()) {
            return;
        }
        for (String key : keys) {
            if (key != null && !key.isEmpty()) {
                bloomFilter.add(key);
            }
        }
    }

    /**
     * 判断元素是否可能存在于布隆过滤器。
     *
     * <p>特性：返回 true 时可能存在误判（元素实际不存在），
     * 返回 false 时元素一定不存在。</p>
     *
     * @param key 元素 key
     * @return true 表示可能存在（可能有误判），false 表示一定不存在
     */
    public boolean contains(String key) {
        if (key == null || key.isEmpty()) {
            return false;
        }
        return bloomFilter.contains(key);
    }

    /**
     * 返回布隆过滤器中近似已添加元素数量。
     *
     * <p>用于容量监控与扩容判定（参见 {@link ScalableBloomFilter#checkAndExpand()}）。</p>
     *
     * @return 近似已添加元素数量
     */
    public long count() {
        return bloomFilter.count();
    }

    /**
     * 返回预期插入量。
     *
     * @return 预期插入量
     */
    public long getExpectedInsertions() {
        return expectedInsertions;
    }
}
